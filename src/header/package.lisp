(in-package #:cl-user)


(defpackage cl-data-frames.header
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.header)
  (:export
   #:alias-to-index
   #:body
   #:column-count
   #:decorate
   #:forward-proxy-frame-range
   #:frame-range-mixin
   #:fundamental-header
   #:index-to-alias
   #:make-header
   #:row-at
   #:rr
   #:standard-header
   #:validate-column-specification
   #:value
   #:with-header))
