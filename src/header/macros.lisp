(in-package #:cl-data-frames.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     ,@body))


(defmacro body (selected-columns &body body)
  (with-gensyms (!arg)
    `(lambda (&rest ,!arg)
       (declare (ignore ,!arg))
       (symbol-macrolet
           ,(mapcar (lambda (binding)
                      (assert (symbolp binding))
                      `(,binding (rr ,binding)))
             selected-columns)
         ,@body))))


(defmacro brr (column)
  `(body () (rr ,column)))
