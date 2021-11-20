(cl:in-package #:vellum.header)


(defmacro with-header ((header) &body body)
  `(let ((*header* ,header)
         (*row* (box nil)))
     (declare (dynamic-extent *row*))
     ,@body))
