(in-package #:cl-user)


(defpackage vellum.table
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.table)
  (:export
   #:*table*
   #:*transform-in-place*
   #:at
   #:column-at
   #:column-count
   #:column-name
   #:column-type
   #:finish-transformation
   #:header
   #:transformation
   #:transform-row
   #:transformation-result
   #:hstack
   #:no-transformation
   #:fundamental-table
   #:standard-table
   #:standard-transformation-row
   #:standard-table-range
   #:setfable-table-row
   #:iterator
   #:drop-row
   #:nullify
   #:remove-nulls
   #:row-count
   #:make-table
   #:make-table*
   #:to-table
   #:transform
   #:vmask
   #:vstack
   #:select
   #:with-table))
