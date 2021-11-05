#|
This example demonstrates how to unnest data using cl-ds.alg:multiplex
|#
(cl:defpackage #:multiplexing-example
  (:use #:cl))

(cl:in-package #:multiplexing-example)

(defparameter *data*
  (vellum:to-table '((a (1 2 3 4))
                     (b (1 2 3 4)))
                   :columns '(shallow-column nested-column)))

(vellum:show :text *data*)
#|
SHALLOW-COLUMN  NESTED-COLUMN
=============================
A               (1 2 3 4)
B               (1 2 3 4)
|#

(defparameter *multiplexed-data*
  (vellum:pipeline (*data*)
    (cl-ds.alg:multiplex :key (vellum:brr nested-column))
    (vellum:to-table :columns '(shallow-column unnested-column)
                     ;; this works, because during row-wise aggregation row special variable continues to be bound, this way we can operate on the returned value in one hand, while still having access to the whole row. When multiplex is active, multiple returned values can be processed in the context of the single row.
                     :key (serapeum:juxt (vellum:brr shallow-column) ; or just (lambda (x) (list (vellum:rr 'shallow-column) x)) if one wants to be explicit
                                         #'identity))))

(vellum:show :text *multiplexed-data*)
#|
SHALLOW-COLUMN  UNNESTED-COLUMN
===============================
A               1
A               2
A               3
A               4
B               1
B               2
B               3
B               4
|#
