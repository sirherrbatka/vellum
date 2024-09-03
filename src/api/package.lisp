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
   #:column-list
   #:pipeline
   #:file-input-row-cant-be-created
   #:drop-row-when
   #:add-columns
   #:tf
   #:order-by
   #:to-matrix
   #:pick-row
   #:rename-columns
   #:s-list
   #:unnest
   #:aggregate-rows
   #:some-null-column-p
   #:every-null-column-p
   #:some-null-column-p*
   #:every-null-column-p*
   #:%aggregate-rows
   #:aggregate-columns
   #:add-row
   #:%aggregate-columns
   #:find-row
   #:found-row
   #:row-cant-be-created)
  (:reexport #:vellum.selection
             #:between
             #:rs
             #:s)
  (:reexport #:cl-ds
             #:replica)
  (:reexport #:vellum.header
             #:header
             #:make-header
             #:skip-row
             #:standard-header
             #:with-header)
  (:reexport #:vellum.table
             #:tablep
             #:*current-row*
             #:*transform-in-place*
             #:aggregate
             #:aggregated-value
             #:alter-columns
             #:at
             #:distinct
             #:retry
             #:bind-row
             #:bind-row-closure
             #:brr
             #:group-by
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
             #:row-to-vector
             #:rr
             #:select
             #:show
             #:vs
             #:to-table
             #:transform
             #:transform-row
             #:transformation
             #:transformation-result
             #:vmask
             #:vstack
             #:vstack*
             #:with-table))
