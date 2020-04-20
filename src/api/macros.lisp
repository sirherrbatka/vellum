(cl:in-package #:vellum)


(defmacro with-standard-header (columns &body body)
  `(with-header ((vellum:make-header 'vellum:standard-header
                                    ,@(mapcar (curry #'list 'quote)
                                              columns)))
     ,@body))
