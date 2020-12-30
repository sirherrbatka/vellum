(cl:in-package #:cl-user)


(defpackage vellum.header
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.header)
  (:export
   #:alias-to-index
   #:no-header
   #:invalid-alias
   #:bind-row
   #:brr
   #:bind-row-closure
   #:column-count
   #:read-alias
   #:column-signature
   #:constantly-t
   #:column-type
   #:*row*
   #:*header*
   #:column-predicate
   #:predicate-failed
   #:concatenate-headers
   #:validated-frame-range-mixin
   #:headers-incompatible
   #:decorate
   #:forward-proxy-frame-range
   #:frame-range-mixin
   #:column-specs
   #:current-row-as-vector
   #:fundamental-header
   #:header
   #:index-to-alias
   #:make-header
   #:no-column
   #:unable-to-construct-row
   #:invalid-input-for-row
   #:row
   #:row-at
   #:rr
   #:select-columns
   #:set-row
   #:skip-row
   #:standard-header
   #:value
   #:with-header))
