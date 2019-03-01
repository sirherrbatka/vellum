(in-package #:cl-user)


(defpackage cl-data-frames.header
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.header)
  (:export
   #:alias-to-index
   #:column-count
   #:fundamental-header
   #:index-to-alias
   #:make-header
   #:standard-header
   #:validate-column-specification))
