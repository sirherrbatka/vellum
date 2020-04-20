(in-package #:vellum.table)


(defmacro with-table ((table) &body body)
  (once-only (table)
    `(let ((*table* ,table))
       (vellum.header:with-header ((header ,table))
         ,@body))))
