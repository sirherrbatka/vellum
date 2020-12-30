(cl:in-package #:cl-user)

(defpackage vellum
  (:use #:cl #:vellum.aux-package)
  (:nicknames #:vellum)
  (:export
   #:add-columns
   #:copy-from
   #:copy-to
   #:empty-column
   #:empty-table
   #:with-standard-header
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
   #:row-cant-be-created
   #:show))

(cl:in-package :vellum)

(rexport :vellum
  cl-ds:replica
  vellum.header:bind-row
  vellum.header:bind-row-closure
  vellum.header:brr
  vellum.header:column-type
  vellum.header:decorate
  vellum.header:header
  vellum.header:make-header
  vellum.header:rr
  vellum.header:skip-row
  vellum.header:standard-header
  vellum.header:with-header
  vellum.table:*transform-in-place*
  vellum.table:*current-row*
  vellum.table:at
  vellum.table:column-count
  vellum.table:drop-row
  vellum.table:erase!
  vellum.table:finish-transformation
  vellum.table:hstack
  vellum.table:make-table
  vellum.table:hstack*
  vellum.table:nullify
  vellum.table:remove-nulls
  vellum.table:row-count
  vellum.table:select
  vellum.table:to-table
  vellum.table:transform
  vellum.table:transform-row
  vellum.table:transformation
  vellum.table:transformation-result
  vellum.table:vmask
  vellum.table:vstack
  vellum.table:vstack*
  vellum.table:with-table)
