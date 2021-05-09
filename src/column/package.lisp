(cl:in-package #:cl-user)


(defpackage vellum.column
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.column)
  (:export
   #:augment-iterator
   #:column-at
   #:column-size
   #:column-type
   #:column-type-error
   #:columns
   #:finish-iterator
   #:fundamental-column
   #:index
   #:iterator-at
   #:make-iterator
   #:make-sparse-material-column
   #:sparse-material-column-iterator-index
   #:move-iterator
   #:move-iterator-to
   #:remove-nulls
   #:sparse-material-column
   #:truncate-to-length
   #:untouch))
