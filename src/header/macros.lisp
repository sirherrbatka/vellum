(cl:in-package #:vellum.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     ,@body))


(defmacro body ((&rest selected-columns) &body body)
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
         (!current-header (gensym "HEADER"))
         (generated (mapcar (lambda (x) (declare (ignore x))
                              (gensym))
                            columns))
         ((:flet generate-column-index (generated column))
          `(setf ,generated
                 ,(cond ((stringp column)
                         `(vellum.header:alias-to-index
                           ,!header
                           ,column))
                        ((symbolp column)
                         `(vellum.header:alias-to-index
                           ,!header
                           ,(symbol-name column)))
                        (t column)))))
    (with-gensyms (!arg)
      `(let (,!header ,@generated)
         (lambda (&rest ,!arg)
           (declare (ignore ,!arg))
           (let ((,!current-header (vellum.header:header)))
             (unless (eq ,!current-header ,!header)
               (setf ,!header ,!current-header)
               ,@(mapcar #'generate-column-index
                         generated
                         columns))
             (let* ((,!row (row))
                    ,@(mapcar (lambda (name column)
                                `(,name (row-at ,!header ,!row ,column)))
                              names
                              generated)
                    ,@(mapcar (lambda (binding gensym)
                                `(,gensym ,binding))
                              names
                              gensyms))
               (declare (special ,@names))
               (prog1 (progn ,@body)
                 ,@(mapcar (lambda (column name gensym)
                             `(unless (eql ,name ,gensym)
                                (setf (row-at ,!header ,!row ,column) ,name)))
                           generated
                           names
                           gensyms)))))))))


(defmacro brr (column)
  (with-gensyms (!header !index !current-header !row)
    `(let ((,!header nil)
           (,!index nil))
       (lambda (&rest all) (declare (ignore all))
         (let ((,!current-header (vellum.header:header))
               (,!row (vellum.header:row)))
           (unless (eq ,!current-header ,!header)
             (setf ,!header ,!current-header
                   ,!index ,(cond ((stringp column)
                                   `(vellum.header:alias-to-index ,!header
                                                                 ,column))
                                  ((symbolp column)
                                   `(vellum.header:alias-to-index ,!header
                                                                 ,(symbol-name column)))
                                  (t column))))
           (row-at ,!header ,!row ,!index))))))
