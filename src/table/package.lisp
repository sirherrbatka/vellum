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
   #:hslice
   #:hstack
   #:remove-nulls
   #:row-count
   #:selection
   #:to-table
   #:transform
   #:vmask
   #:vslice
   #:vstack
   #:with-table))
