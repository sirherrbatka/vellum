(in-package #:cl-user)


(defpackage cl-data-frames.header
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.header)
  (:export
   #:alias-to-index
   #:column-count
   #:decorate-data
   #:fundamental-header
   #:index-to-alias
   #:make-header
   #:row-at
   #:standard-header
   #:validate-column-specification))
