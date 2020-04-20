(in-package #:cl-user)


(defpackage vellum.table
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.table)
  (:export
   #:*table*
   #:*transform-in-place*
   #:at
   #:column-count
   #:column-name
   #:column-type
   #:finish-transformation
   #:header
   #:transformation
   #:transform-row
   #:transformation-result
   #:hselect
   #:hstack
   #:no-transformation
   #:fundamental-table
   #:standard-table
   #:standard-table-range
   #:setfable-table-row
   #:iterator
   #:drop-row
   #:nullify
   #:remove-nulls
   #:row-count
   #:selection
   #:make-table
   #:to-table
   #:transform
   #:vmask
   #:vselect
   #:vstack
   #:with-table))
