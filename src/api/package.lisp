(cl:in-package #:cl-user)

(cl-ds.fancy:defpackage vellum
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
   #:file-input-row-cant-be-created
   #:add-columns
   #:order-by
   #:to-matrix
   #:aggregate-rows
   #:%aggregate-rows
   #:aggregate-columns
   #:%aggregate-columns
   #:row-cant-be-created)
  (:reexport #:vellum.selection
             #:between
             #:rs
             #:s
             #:vs)
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
             #:*current-row*
             #:*transform-in-place*
             #:alter-columns
             #:at
             #:column-count
             #:column-name
             #:column-names
             #:column-type
             #:drop-row
             #:erase!
             #:finish-transformation
             #:hstack
             #:hstack*
             #:make-table
             #:nullify
             #:remove-nulls
             #:row-count
             #:row-to-list
             #:select
             #:show
             #:to-table
             #:transform
             #:transform-row
             #:transformation
             #:transformation-result
             #:vmask
             #:vstack
             #:vstack*
             #:with-table))
