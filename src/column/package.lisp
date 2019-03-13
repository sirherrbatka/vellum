(in-package #:cl-user)


(defpackage cl-data-frames.column
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.column)
  (:export
   #:augment-iterator
   #:column-at
   #:column-size
   #:column-type
   #:complex-iterator
   #:finish-iterator
   #:truncate-to-length
   #:fundamental-column
   #:fundamental-iterator
   #:fundamental-pure-iterator
   #:iterator-at
   #:make-iterator
   #:make-sparse-material-column
   #:move-iterator
   #:sparse-material-column))

(cl-data-frames.aux-package:rexport :cl-data-frames.column
  cl-ds:erase!
  cl-ds:update!
  cl-ds:add!
  cl-ds:update-if!
  cl-ds:replica
  cl-ds:put-back!)
