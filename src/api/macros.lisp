(cl:in-package #:cl-df)


(defmacro with-standard-header (columns &body body)
  `(with-header ((cl-df:make-header 'cl-df:standard-header
                                    ,@(mapcar (curry #'list 'quote)
                                              columns)))
     ,@body))
