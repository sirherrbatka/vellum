(in-package #:cl-data-frames.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     ,@body))


(defmacro body (&body body)
  (with-gensyms (!arg)
    `(lambda (,!arg)
       (declare (ignore ,!arg))
       ,@body)))
