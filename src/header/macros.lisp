(cl:in-package #:vellum.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     ,@body))


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
         (!header (gensym "HEADER"))
         (generated (mapcar (lambda (x) (declare (ignore x))
                              (gensym))
                            columns))
         ((:flet generate-column-index (generated column))
          `(,generated
            ,(cond ((stringp column)
                    `(vellum.header:name-to-index
                      ,!header
                      ,column))
                   ((symbolp column)
                    `(vellum.header:name-to-index
                      ,!header
                      ,(symbol-name column)))
                   (t column)))))
    (with-gensyms (!arg)
      `(make-bind-row
        (lambda (&optional (,!header (vellum.header:header)))
          (declare (ignorable ,!header))
          (let ,(mapcar #'generate-column-index
                  generated
                  columns)
             (lambda (&rest ,!arg)
               (declare (ignore ,!arg))
               (let* ((,!row (row))
                      ,@(mapcar (lambda (name column)
                                  `(,name (row-at ,!header ,!row ,column)))
                                names
                                generated)
                      ,@(mapcar #'list
                                gensyms
                                names))
                 (declare (ignorable ,!row))
                 (prog1 (progn ,@body)
                   ,@(mapcar (lambda (column name gensym)
                               `(unless (eql ,name ,gensym)
                                  (setf (row-at ,!header ,!row ,column) ,name)))
                             generated
                             names
                             gensyms))))))
        (lambda (&rest ,!arg)
          (declare (ignore ,!arg))
          (let* ((,!header (vellum.header:header))
                 ,@(mapcar #'generate-column-index
                           generated
                           columns)
                 (,!row (row))
                 ,@(mapcar (lambda (name column)
                             `(,name (row-at ,!header ,!row ,column)))
                           names
                           generated)
                 ,@(mapcar #'list
                           gensyms
                           names))
            (declare (ignorable ,!header ,!row))
            (prog1 (progn ,@body)
              ,@(mapcar (lambda (column name gensym)
                          `(unless (eql ,name ,gensym)
                             (setf (row-at ,!header ,!row ,column) ,name)))
                        generated
                        names
                        gensyms))))))))


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
                                      (t column)))))
             (row-at ,!header ,!row ,!index)))
        (let* ((columns (cons column other-columns))
               (indexes (~> columns
                            length
                            make-list
                            (map-into #'gensym))))
          `(let (,!header ,@indexes)
             (lambda (&rest ,!all) (declare (ignore ,!all))
               (unless (eq ,!current-header ,!header)
                 (setf ,!header ,!current-header
                       ,@(iterate
                           (for column in columns)
                           (for index in indexes)
                           (collecting index)
                           (collecting `(cond ((stringp ,column)
                                               `(vellum.header:name-to-index ,!header
                                                                             ,column))
                                              ((symbolp ,column)
                                               `(vellum.header:name-to-index ,!header
                                                                             ,(symbol-name column)))
                                              (t column))))))
               (list ,@(iterate
                         (for index in indexes)
                         (collecting `(row-at ,!header ,!row ,index))))))))))
