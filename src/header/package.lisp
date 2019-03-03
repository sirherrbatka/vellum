(in-package #:cl-user)


(defpackage cl-data-frames.header
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.header)
  (:export
   #:alias-to-index
   #:column-count
   #:fundamental-header
   #:frame-range-mixin
   #:index-to-alias
   #:make-header
   #:row-at
   #:standard-header
   #:validate-column-specification))
