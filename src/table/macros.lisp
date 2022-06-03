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


(defmethod proxy-aggregator* ((name (eql 'distinct))
                             options
                             aggregator
                             rest)
  (proxy-aggregator `(cl-ds.alg.meta:layer-aggregator-constructor
                       (function cl-ds.alg:distinct)
                       ,aggregator
                       (list ,@options))
                    rest))


(defun rewrite-bind-row-form (form)
  (let* ((gathered-constructor-forms (make-hash-table :test 'eql))
         (gathered-constructor-variables (make-hash-table :test 'eql))
         (pre-form nil)
         (result
           (agnostic-lizard:walk-form
            form
            nil
            :on-every-form-pre
            (lambda (f e) (declare (ignore e)) (setf pre-form f))
            :on-macroexpanded-form
            (lambda (f e) (declare (ignore e))
              (if (and (listp pre-form) (eq (car pre-form) 'vellum.table::aggregate))
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
                    (check-type into symbol)
                    (assert (or (null old-form)
                                (equalp old-form constructor-form)))
                    `(cl-ds.alg.meta:pass-to-aggregation ,constructor-variable
                                                         ,what))
                  f)))))
    (values result
            gathered-constructor-variables
            gathered-constructor-forms)))


(defmacro aggregate (into (name what &rest options) &body body)
  (declare (ignore into name what options body))
  `(error "Aggregation not allowed in this call."))


(defmacro bind-row (selected-columns &body body)
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
         ((:values bind-row-form
                   constructor-variables
                   constructor-forms)
          (rewrite-bind-row-form
           `(macrolet ((aggregate (into (name what &rest options) &body body)
                         (declare (ignore options name body))
                         `(cl-ds.alg.meta:pass-to-aggregation ,what ,into)))
              (prog1 (progn ,@body)
                ,@(mapcar (lambda (column name gensym)
                            `(unless (eql ,name ,gensym)
                               (setf (row-at vellum.header:*header* ,!row ,column) ,name)))
                          generated
                          names
                          gensyms)))))
         (let-constructors
          (iterate
            (for (key value) in-hashtable constructor-variables)
            (collecting (list value `(cl-ds.alg.meta:call-constructor ,(gethash key constructor-forms)))))))
    `(make-bind-row
      (lambda (&optional (vellum.header:*header* (vellum.header:header)))
        (let (,@(mapcar #'generate-column-index
                        generated
                        columns)
              ,@let-constructors)
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
           (list ,@(iterate
                     (for (key value) in-hashtable constructor-variables)
                     (collecting `(list (quote ,key) ,value)))))))
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
