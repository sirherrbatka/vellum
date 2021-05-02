(cl:in-package #:cl-user)


(defpackage vellum.table
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum.table)
  (:export
   #:*current-row*
   #:*table*
   #:*transform-in-place*
   #:at
   #:bind-row-closure
   #:column-at
   #:column-count
   #:column-name
   #:column-type
   #:drop-row
   #:erase!
   #:finish-transformation
   #:retry
   #:fundamental-table
   #:header
   #:hstack
   #:hstack*
   #:iterator
   #:make-table
   #:make-table*
   #:no-transformation
   #:nullify
   #:remove-nulls
   #:row-count
   #:select
   #:skip-row
   #:setfable-table-row
   #:show
   #:standard-table
   #:standard-table-range
   #:standard-transformation-row
   #:to-table
   #:transform
   #:transform-row
   #:transformation
   #:transformation-error
   #:transformation-result
   #:vmask
   #:vstack
   #:vstack*
   #:with-table))
