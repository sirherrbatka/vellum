(cl:in-package #:vellum.table)


(defmacro with-table ((table) &body body)
  (once-only (table)
    `(let ((*table* ,table))
       (vellum.header:with-header ((header ,table))
         ,@body))))


(defgeneric proxy-aggregator* (name options aggregator rest))


(defun proxy-aggregator (aggregator rest)
  (if (endp rest)
      aggregator
      (bind ((((name . options) . rest) rest))
        (proxy-aggregator* name options aggregator rest))))


(defmethod proxy-aggregator* ((name t)
                             options
                             aggregator
                             rest)
  (proxy-aggregator `(cl-ds.alg.meta:layer-aggregator-constructor
                       (function ,name)
                       ,aggregator
                       (list ,@options))
                    rest))


(defmacro aggregate (into function &rest body)
  (declare (ignore into function))
  `(progn (error "Aggregation not allowed in current context.")
          ,@body))


(defmacro aggregated-value (into)
  (declare (ignore into))
  `(error "Aggregation not allowed in current context."))


(defmacro group-by (&rest column-names)
  `(progn ,@column-names))


(defmacro distinct (&rest data)
  `(progn ,@data))


(defclass aggregation-results ()
  ((%aggregators :initarg :aggregators
                 :reader aggregators)
   (%aggregation-column-names :initarg :aggregation-column-names
                              :reader aggregation-column-names)))


(defclass group-by-aggregation-results ()
  ((%aggregators :initarg :aggregators
                 :reader aggregators)
   (%aggregation-column-names :initarg :aggregation-column-names
                              :reader aggregation-column-names)
   (%group-names :initarg :group-names
                 :reader group-names)))


(defun rewrite-bind-row-form-pass-1 (form env)
  (let* ((gathered-constructor-forms (make-hash-table :test 'eql))
         (gathered-constructor-variables (make-hash-table :test 'eql))
         (gathered-group-by-variables (list))
         (gathered-distinct-variables (make-hash-table :test 'eql))
         (group-names (list))
         (pre-form nil)
         (aggregation-symbol (gensym))
         (extract-value-symbol (gensym))
         (result
           (agnostic-lizard:walk-form
            form
            env
            :on-every-form-pre
            (lambda (f e) (declare (ignore e)) (setf pre-form f))
            :on-macroexpanded-form
            (lambda (f e) (declare (ignore e))
              (cond ((and (listp pre-form) (eq (car pre-form) 'vellum.table:aggregate))
                     (bind (((into (name what . options) . proxies) (rest pre-form))
                            (constructor-form (proxy-aggregator `(cl-ds.alg.meta:aggregator-constructor
                                                                  '() nil
                                                                  (function ,name)
                                                                  (list '() ,@options))
                                                                proxies))
                            (old-form (shiftf (gethash into gathered-constructor-forms)
                                              constructor-form))
                            (constructor-variable (ensure (gethash into gathered-constructor-variables)
                                                    (gensym))))
                       (check-type into (or symbol string))
                       (assert (or (null old-form)
                                   (equalp old-form constructor-form)))
                       `(,aggregation-symbol ,constructor-variable
                                             ,what
                                             ,constructor-form
                                             ,into)))
                    ((and (listp pre-form) (eq (car pre-form) 'vellum.table:distinct))
                     (let ((symbol (gensym)))
                       (setf (gethash symbol gathered-distinct-variables) (rest pre-form))
                       `(when (shiftf (gethash ,(econd ((= 1 (length (rest pre-form)))
                                                        (second pre-form))
                                                       ((> (length (rest pre-form)) 1)
                                                        `(list ,@(rest pre-form))))
                                               ,symbol)
                                      t)
                          (drop-row))))
                    ((and (listp pre-form) (eq (car pre-form) 'vellum.table:group-by))
                     (iterate
                       (for elt in (rest pre-form))
                       (if (listp elt)
                           (progn
                             (push (second elt) gathered-group-by-variables)
                             (push (first elt) group-names))
                           (progn
                             (push elt group-names)
                             (push elt gathered-group-by-variables)))))
                    ((and (listp pre-form) (eq (car pre-form) 'vellum.table:aggregated-value))
                     (bind (((macro-name into) pre-form)
                            (constructor-form (gethash into gathered-constructor-forms)))
                       (declare (ignore macro-name))
                       `(,extract-value-symbol ,(ensure (gethash into gathered-constructor-variables)
                                                  (gensym))
                                               ,into
                                               ,constructor-form)))
                    (t f))))))
    (list result
          gathered-constructor-variables
          gathered-constructor-forms
          aggregation-symbol
          extract-value-symbol
          gathered-distinct-variables
          (nreverse gathered-group-by-variables)
          (nreverse group-names))))


(defun rewrite-bind-row-form-pass-2 (result
                                     gathered-constructor-variables
                                     gathered-constructor-forms
                                     aggregation-symbol
                                     extract-value-symbol
                                     gathered-distinct-variables
                                     gathered-group-by-variables
                                     group-names)
  (let* ((!grouped-aggregators (gensym)))
    (list (serapeum:map-tree
           (lambda (f)
             (cond ((and (listp f) (eq (first f) aggregation-symbol))
                    (bind (((constructor-variable what constructor-form result-name) (rest f)))
                      (if (endp group-names)
                          `(cl-ds.alg.meta:pass-to-aggregation ,constructor-variable ,what)
                          (with-gensyms (!group !aggregators !group-key)
                            `(let* ((,!group-key (list ,@gathered-group-by-variables))
                                    (,!group (ensure (gethash ,!group-key ,!grouped-aggregators)
                                               (make-hash-table :test 'equal)))
                                    (,!aggregators (ensure (gethash ',result-name ,!group)
                                                     (cl-ds.alg.meta:call-constructor ,constructor-form))))
                               (cl-ds.alg.meta:pass-to-aggregation ,!aggregators ,what))))))
                   ((and (listp f) (eq (first f) extract-value-symbol))
                    (bind (((constructor-variable result-name constructor-form) (rest f)))
                      (if (endp group-names)
                          `(cl-ds.alg.meta:extract-result ,constructor-variable)
                          (with-gensyms (!group-key !aggregators !group)
                            `(let* ((,!group-key (list ,@gathered-group-by-variables))
                                    (,!group (ensure (gethash ,!group-key ,!grouped-aggregators)
                                               (make-hash-table :test 'equal)))
                                    (,!aggregators (or (gethash ',result-name ,!group)
                                                       (cl-ds.alg.meta:call-constructor ,constructor-form))))
                               (cl-ds.alg.meta:extract-result ,!aggregators))))))
                   (t f)))
           result)
          gathered-constructor-variables
          gathered-constructor-forms
          aggregation-symbol
          extract-value-symbol
          gathered-distinct-variables
          gathered-group-by-variables
          group-names
          !grouped-aggregators)))


(defun rewrite-bind-row-form (form env)
  (~>> (rewrite-bind-row-form-pass-1 form env)
       (apply #'rewrite-bind-row-form-pass-2)))


(defmacro bind-row (selected-columns &body body &environment env)
  (bind ((gensyms (mapcar (lambda (x) (declare (ignore x)) (gensym))
                         selected-columns))
         (names (mapcar (lambda (x)
                          (econd
                            ((symbolp x) x)
                            ((listp x) (first x))))
                        selected-columns))
         (columns (mapcar (lambda (x)
                            (econd
                              ((symbolp x) x)
                              ((stringp x) x)
                              ((listp x) (second x))))
                          selected-columns))
         (!row (gensym "ROW"))
         (!rest (gensym "REST"))
         (generated (mapcar (lambda (x) (declare (ignore x))
                              (gensym))
                            columns))
         ((:flet generate-column-index (generated column))
          `(,generated
            ,(cond ((stringp column)
                    `(vellum.header:name-to-index
                      vellum.header:*header*
                      ,column))
                   ((symbolp column)
                    `(vellum.header:name-to-index
                      vellum.header:*header*
                      ,(symbol-name column)))
                   (t column))))
         ((bind-row-form
           constructor-variables
           constructor-forms
           aggregation-symbol
           extract-value-symbol
           gathered-distinct-variables
           gathered-group-by-variables
           group-names
           !grouped-aggregators)
          (rewrite-bind-row-form
           `(prog1 (progn ,@body)
              ,@(mapcar (lambda (column name gensym)
                          `(unless (eql ,name ,gensym)
                             (setf (row-at vellum.header:*header* ,!row ,column) ,name)))
                        generated
                        names
                        gensyms))
           env))
         (let-constructors
          (iterate
            (for (key value) in-hashtable constructor-variables)
            (collecting (list value `(cl-ds.alg.meta:call-constructor ,(gethash key constructor-forms))))))
         (let-distinct
          (iterate
            (for (key value) in-hashtable gathered-distinct-variables)
            (collecting (list key `(make-hash-table :test 'equal))))))
    (declare (ignore extract-value-symbol aggregation-symbol))
    (unless (= (length (remove-duplicates group-names :test #'equal))
               (length group-names))
      (error 'program-error "Duplicated group name detected!"))
    `(make-bind-row
      (lambda (&optional (vellum.header:*header* (vellum.header:header)))
        (let (,@(mapcar #'generate-column-index
                        generated
                        columns)
              ,@(if (endp gathered-group-by-variables)
                    let-constructors
                    `((,!grouped-aggregators (make-hash-table :test 'equal))))
              ,@let-distinct)
          (values
           (lambda (&optional (,!row (vellum.header:row)))
             (declare (ignorable ,!row)
                      (type table-row ,!row))
             (let* (,@(mapcar (lambda (name column)
                                `(,name (row-at vellum.header:*header* ,!row ,column)))
                              names
                              generated)
                    ,@(mapcar #'list
                              gensyms
                              names))
               ,bind-row-form))
           ,(if (zerop (hash-table-count constructor-variables))
               nil
               (if (endp gathered-group-by-variables)
                   `(make-instance 'aggregation-results
                                   :aggregators (list ,@(iterate
                                                          (for (key value) in-hashtable constructor-variables)
                                                          (collecting value)))
                                   :aggregation-column-names '(,@(iterate
                                                                   (for (key value) in-hashtable constructor-variables)
                                                                   (collecting key))))
                   `(make-instance 'group-by-aggregation-results
                                   :aggregators ,!grouped-aggregators
                                   :group-names '(,@group-names)
                                   :aggregation-column-names '(,@(iterate
                                                                   (for (key value) in-hashtable constructor-variables)
                                                                   (collecting key)))))))))
      (lambda (&rest ,!rest)
        (declare (ignore ,!rest))
        (let* ((vellum.header:*header* (vellum.header:header))
               ,@(mapcar #'generate-column-index
                         generated
                         columns)
               (,!row (vellum.header:row))
               ,@(mapcar (lambda (name column)
                           `(,name (row-at vellum.header:*header* ,!row ,column)))
                         names
                         generated)
               ,@(mapcar #'list
                         gensyms
                         names))
          (declare (ignorable ,!row))
          (prog1 (progn ,@body)
            ,@(mapcar (lambda (column name gensym)
                        `(unless (eql ,name ,gensym)
                           (setf (row-at vellum.header:*header* ,!row ,column) ,name)))
                      generated
                      names
                      gensyms)))))))


(defmacro brr (column &rest other-columns)
  (with-gensyms (!header !index !current-header !row !all)
    (if (endp other-columns)
        `(let ((,!header nil)
               (,!index nil))
           (lambda (&rest ,!all) (declare (ignore ,!all))
             (let ((,!current-header (vellum.header:header))
                   (,!row (vellum.header:row)))
               (unless (eq ,!current-header ,!header)
                 (setf ,!header ,!current-header
                       ,!index ,(cond ((stringp column)
                                       `(vellum.header:name-to-index ,!header
                                                                     ,column))
                                      ((symbolp column)
                                       `(vellum.header:name-to-index ,!header
                                                                     ,(symbol-name column)))
                                      (t column))))
               (row-at ,!header ,!row ,!index))))
        (let* ((columns (cons column other-columns))
               (indexes (~> columns
                            length
                            make-list
                            (map-into #'gensym))))
          `(let (,!header ,@indexes)
             (lambda (&optional (,!row (vellum.header:row)))
               (let ((,!current-header (vellum.header:header)))
                 (unless (eq ,!current-header ,!header)
                   (setf ,!header ,!current-header
                         ,@(iterate
                             (for column in columns)
                             (for index in indexes)
                             (collecting index)
                             (collecting (cond ((stringp column)
                                                `(vellum.header:name-to-index ,!header
                                                                              ,column))
                                               ((symbolp column)
                                                `(vellum.header:name-to-index ,!header
                                                                              ,(symbol-name column)))
                                               (t column))))))
                 (list ,@(iterate
                           (for index in indexes)
                           (collecting `(row-at ,!header ,!row ,index)))))))))))


(defmacro br (&body body &environment env)
  (bind ((column-vars '())
         ((:flet strip (symbol))
          (~>> symbol symbol-name (drop 1) intern))
         ((:flet strip-check (f &rest ignored))
          (declare (ignore ignored))
          (if (member f column-vars) (strip f) f))
         ((:flet nothing (f e)) (declare (ignore e)) f)
         ((:flet symbol-transformer (f e))
          (unless (or (not (symbolp f))
                      (find f (agnostic-lizard:metaenv-variable-like-entries e) :key 'first)
                      (not (char-equal #\$ (~> f symbol-name first-elt))))
            (pushnew f column-vars))
          (strip-check f))
         ((:flet setq-hack (f e))
          (cond
            ((eql (first f) 'setq)
             `(setq ,(symbol-transformer (second f) e)
                    ,(third f)))
            (t f)))
         ((:labels walk (f e &aux (pre-form nil)))
          (agnostic-lizard:walk-form
           f e
           :on-special-form #'setq-hack
           :on-macroexpanded-form
           (lambda (f e)
             (if (and (listp pre-form)
                      (member (car pre-form)
                              '(vellum.table:aggregate vellum.table:group-by vellum.table:distinct)))
                 (progn (walk f e)      ; this will simply gather column names
                        (third ; silly trick, stops macro expansion of agnostic-lizard :-)
                         (agnostic-lizard:walk-form `(flet ((vellum.table:aggregate (&rest body) nil)
                                                            (vellum.table:group-by (&rest body) nil)
                                                            (vellum.table:distinct (&rest body) nil))
                                                       ,pre-form)
                                                    e
                                                    :on-every-form-pre  #'nothing
                                                    :on-macroexpanded-form #'nothing
                                                    :on-every-atom #'symbol-transformer)))
                 f))
           :on-every-form-pre
           (lambda (f e) (declare (ignore e))
             (setf pre-form f))
           :on-every-atom #'symbol-transformer))
         (result (walk `(progn ,@body) env)))
    `(bind-row ,(mapcar #'strip column-vars)
       ,@(rest result))))
