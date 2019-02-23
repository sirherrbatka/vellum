(in-package #:cl-user)

(defpackage cl-data-frames.header
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.header)
  (:export
   #:alias-to-index
   #:fundamental-header
   #:index-to-alias
   #:standard-header))
