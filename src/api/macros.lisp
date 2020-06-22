(cl:in-package #:vellum)


(defmacro with-standard-header (columns &body body)
  `(with-header ((vellum:make-header 'vellum:standard-header
                                    ,@(mapcar (curry #'list 'quote)
                                              columns)))
     ,@body))


(defmacro pipeline ((table) &body body)
  (once-only (table)
    `(with-table (,table)
       (~> (cl-ds:whole-range ,table)
           ,@body))))
