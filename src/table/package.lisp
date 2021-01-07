(cl:in-package #:cl-user)


(defpackage vellum.table
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.table)
  (:export
   #:*current-row*
   #:*table*
   #:*transform-in-place*
   #:at
   #:column-at
   #:column-count
   #:column-name
   #:column-type
   #:drop-row
   #:erase!
   #:finish-transformation
   #:fundamental-table
   #:header
   #:hstack
   #:hstack*
   #:iterator
   #:make-table
   #:make-table*
   #:no-transformation
   #:show
   #:nullify
   #:remove-nulls
   #:bind-row-closure
   #:row-count
   #:select
   #:setfable-table-row
   #:standard-table
   #:standard-table-range
   #:standard-transformation-row
   #:to-table
   #:transform
   #:transform-row
   #:transformation
   #:transformation-result
   #:vmask
   #:vstack
   #:vstack*
   #:with-table))
