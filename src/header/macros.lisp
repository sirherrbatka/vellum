(in-package #:cl-data-frames.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     ,@body))


(defmacro body (selected-columns &body body)
  (let ((gensyms (mapcar (lambda (x) (declare (ignore x))
                           (gensym)) selected-columns)))
    (with-gensyms (!arg)
      `(lambda (&rest ,!arg)
         (declare (ignore ,!arg))
         (let* (,@(mapcar (lambda (binding)
                            (assert (symbolp binding))
                            `(,binding (rr ,binding)))
                          selected-columns)
                ,@(mapcar (lambda (binding gensym)
                            `(,binding ,gensym))
                          selected-columns
                          gensyms))
           (declare (special ,@selected-columns))
           ,@body
           ,@(mapcar (lambda (binding gensym)
                       `(unless (eql ,binding ,gensym)
                          (setf (rr ,binding) ,gensym)))
                     selected-columns
                     gensyms))))))


(defmacro brr (column)
  `(body () (rr ,column)))
