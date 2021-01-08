(cl:in-package #:cl-user)


(defpackage vellum.header
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.header)
  (:export
   #:*header*
   #:*row*
   #:bind-row
   #:bind-row-closure
   #:brr
   #:column-count
   #:column-predicate
   #:column-signature
   #:read-header
   #:column-specs
   #:check-predicate
   #:ensure-index
   #:column-type
   #:concatenate-headers
   #:constantly-t
   #:current-row-as-vector
   #:frame-range-mixin
   #:fundamental-header
   #:header
   #:headers-incompatible
   #:index-to-name
   #:invalid-input-for-row
   #:invalid-name
   #:make-header
   #:name-duplicated
   #:name-to-index
   #:no-column
   #:no-header
   #:predicate-failed
   #:read-name
   #:row
   #:row-at
   #:rr
   #:select-columns
   #:set-row
   #:skip-row
   #:standard-header
   #:unable-to-construct-row
   #:validated-frame-range-mixin
   #:value
   #:with-header))
