(in-package #:cl-data-frames.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     ,@body))


(defmacro body ((&rest selected-columns) &body body)
  (let ((gensyms (mapcar (lambda (x) (declare (ignore x)) (gensym))
                         selected-columns))
        (names (mapcar (lambda (x)
                         (econd
                           ((symbolp x) x)
                           ((listp x) (first x))))
                       selected-columns))
        (columns (mapcar (lambda (x)
                           (econd
                             ((symbolp x) x)
                             ((listp x) (second x))))
                         selected-columns)))
    (with-gensyms (!arg)
      `(lambda (&rest ,!arg)
         (declare (ignore ,!arg))
         (let* (,@(mapcar (lambda (name column)
                            `(,name (rr ',column)))
                          names
                          columns)
                ,@(mapcar (lambda (binding gensym)
                            `(,gensym ,binding))
                          names
                          gensyms))
           (declare (special ,@selected-columns))
           (prog1 (progn ,@body)
             ,@(mapcar (lambda (column name gensym)
                         `(unless (eql ,name ,gensym)
                            (setf (rr ',column) ,name)))
                       columns
                       names
                       gensyms)))))))


(defmacro brr (column)
  `(lambda (&rest all) (declare (ignore all))
     (rr ',column)))
