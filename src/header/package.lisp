(cl:in-package #:cl-user)


(defpackage vellum.header
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.header)
  (:export
   #:*header*
   #:*row*
   #:alter-columns
   #:check-column-signatures-compatibility
   #:column-count
   #:column-names
   #:column-signature
   #:column-specs
   #:column-type
   #:concatenate-headers
   #:ensure-index
   #:frame-range-mixin
   #:header
   #:headers-incompatible
   #:index-to-name
   #:invalid-input-for-row
   #:write-header
   #:invalid-name
   #:keep-old-value
   #:make-header
   #:make-row
   #:name-duplicated
   #:name-to-index
   #:no-column
   #:no-header
   #:provide-new-value
   #:read-header
   #:read-name
   #:read-new-value
   #:row
   #:select-columns
   #:set-row
   #:set-to-null
   #:skip-row
   #:skip-row
   #:standard-header
   #:unable-to-construct-row
   #:validated-frame-range-mixin
   #:value
   #:with-header))
