(in-package #:cl-data-frames.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     ,@body))


(defmacro body (&body body)
  (with-gensyms (!arg)
    `(lambda (&rest ,!arg)
       (declare (ignore ,!arg))
       ,@body)))


(defmacro brr (column)
  `(body (rr ,column)))
