(in-package #:cl-user)

(defpackage cl-data-frames.table
  (:use #:cl #:cl-data-frames.aux-package)
  (:nicknames #:cl-df.table)
  (:export
   #:*table*
   #:at
   #:column-count
   #:column-name
   #:column-type
   #:header
   #:hmask
   #:hslice
   #:hstack
   #:row-count
   #:to-table
   #:transform
   #:vslice
   #:vstack
   #:with-table))
