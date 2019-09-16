(in-package #:cl-user)


(defpackage cl-data-frames.header
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.header)
  (:export
   #:alias-to-index
   #:body
   #:brr
   #:column-count
   #:column-type
   #:column-predicate
   #:predicate-failed
   #:concatenate-headers
   #:validated-frame-range-mixin
   #:headers-incompatible
   #:decorate
   #:forward-proxy-frame-range
   #:frame-range-mixin
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
