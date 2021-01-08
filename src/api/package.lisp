(cl:in-package #:cl-user)

(vellum.utils:def-fancy-package vellum
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum)
  (:export
   #:add-columns
   #:copy-from
   #:copy-to
   #:empty-column
   #:empty-table
   #:with-standard-header
   #:visualize
   #:join
   #:pipeline
   #:alter-columns
   #:file-input-row-cant-be-created
   #:new-columns
   #:order-by
   #:to-matrix
   #:aggregate-rows
   #:%aggregate-rows
   #:aggregate-columns
   #:%aggregate-columns
   #:row-cant-be-created)
  (:reexport #:vellum.selection
             #:s
             #:rs
             #:vs
             #:between)
  (:reexport #:cl-ds
             #:replica)
  (:reexport #:vellum.header
             #:bind-row
             #:bind-row-closure
             #:brr
             #:header
             #:make-header
             #:rr
             #:skip-row
             #:standard-header
             #:with-header)
  (:reexport #:vellum.table
             #:*transform-in-place*
             #:*current-row*
             #:at
             #:column-count
             #:column-type
             #:column-name
             #:drop-row
             #:erase!
             #:finish-transformation
             #:hstack
             #:show
             #:make-table
             #:hstack*
             #:nullify
             #:remove-nulls
             #:row-count
             #:select
             #:to-table
             #:transform
             #:transform-row
             #:transformation
             #:transformation-result
             #:vmask
             #:vstack
             #:vstack*
             #:with-table))
