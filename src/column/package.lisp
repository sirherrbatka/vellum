(cl:in-package #:cl-user)


(defpackage vellum.column
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.column)
  (:export
   #:augment-iterator
   #:columns
   #:column-at
   #:column-size
   #:column-type
   #:column-type-error
   #:finish-iterator
   #:truncate-to-length
   #:remove-nulls
   #:index
   #:fundamental-column
   #:iterator-at
   #:make-iterator
   #:make-sparse-material-column
   #:move-iterator
   #:move-iterator-to
   #:sparse-material-column))
