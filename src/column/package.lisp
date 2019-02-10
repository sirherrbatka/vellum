(in-package #:cl-user)


(defpackage cl-data-frames.column
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.column)
  (:export
   #:column-type
   #:column-size
   #:column-at
   #:replica
   #:make-iterator
   #:iterator-at
   #:fundamental-column
   #:fundamental-iterator
   #:fundamental-pure-iterator
   #:make-sparse-material-column
   #:complex-iterator
   #:sparse-material-column
   #:move-iterator
   #:augment-iterator
   #:finish-iterator))

(cl-data-frames.aux-package:rexport :cl-data-frames.column
  cl-ds:erase!
  cl-ds:update!
  cl-ds:add!
  cl-ds:update-if!
  cl-ds:replica
  cl-ds:put-back!)
