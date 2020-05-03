(in-package #:vellum.table)

(prove:plan 42)

(progn
  (defparameter *test-data* #(#(1 a 5 s)
                              #(2 b 6 s)
                              #(3 c 7 s)))

  (vellum:with-header ((vellum:make-header 'vellum:standard-header
                                           nil nil nil nil))
    (defparameter *table*
      (~> *test-data*
          cl-ds:whole-range
          vellum:decorate
          vellum:to-table)))

  (prove:is (vellum:at *table* 0 0) 1)
  (prove:is (vellum:at *table* 0 1) 2)
  (prove:is (vellum:at *table* 0 2) 3)

  (prove:is (vellum:at *table* 1 0) 'a)
  (prove:is (vellum:at *table* 1 1) 'b)
  (prove:is (vellum:at *table* 1 2) 'c)

  (prove:is (vellum:at *table* 2 0) 5)
  (prove:is (vellum:at *table* 2 1) 6)
  (prove:is (vellum:at *table* 2 2) 7)

  (prove:is (vellum:at *table* 3 0) 's)
  (prove:is (vellum:at *table* 3 1) 's)
  (prove:is (vellum:at *table* 3 2) 's)

  (defparameter *replica*
    (vellum:transform *table*
                     (vellum:body ()
                       (setf (vellum:rr 0) (+ 1 (vellum:rr 0))))
                     :in-place nil))

  (prove:is (vellum:at *table* 0 0) 1)
  (prove:is (vellum:at *table* 0 1) 2)
  (prove:is (vellum:at *table* 0 2) 3)

  (prove:is (vellum:at *replica* 0 0) 2)
  (prove:is (vellum:at *replica* 0 1) 3)
  (prove:is (vellum:at *replica* 0 2) 4)

  (vellum:transform *table*
                   (vellum:body ()
                     (setf (vellum:rr 0) (* 2 (vellum:rr 0))))
                   :in-place t)

  (prove:is (vellum:at *table* 0 0) 2)
  (prove:is (vellum:at *table* 0 1) 4)
  (prove:is (vellum:at *table* 0 2) 6)

  (prove:is (vellum:at *replica* 0 0) 2)
  (prove:is (vellum:at *replica* 0 1) 3)
  (prove:is (vellum:at *replica* 0 2) 4)

  (defparameter *concatenated-table* (vellum:vstack *table*
                                                   (list *replica*)))
  (prove:is (column-count *concatenated-table*) 4)
  (prove:is (row-count *concatenated-table*) 6)

  (prove:is (vellum:at *concatenated-table* 0 0) 2)
  (prove:is (vellum:at *concatenated-table* 0 1) 4)
  (prove:is (vellum:at *concatenated-table* 0 2) 6)
  (prove:is (vellum:at *concatenated-table* 0 3) 2)
  (prove:is (vellum:at *concatenated-table* 0 4) 3)
  (prove:is (vellum:at *concatenated-table* 0 5) 4)

  (defparameter *sub-table* (vselect *concatenated-table* (selection 1 4)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 0 1) 6)
  (prove:is (at *sub-table* 0 2) 2)

  (defparameter *sub-table* (vselect *concatenated-table* (iota 3 :start 1)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 0 1) 6)
  (prove:is (at *sub-table* 0 2) 2))

(prove:finalize)
