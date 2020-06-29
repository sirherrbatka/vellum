(in-package #:vellum.table)

(prove:plan 44)

(progn
  (defparameter *test-data* #(#(1 a 5 s)
                              #(2 b 6 s)
                              #(3 c 7 s)))

  (vellum:with-standard-header (nil nil nil nil)
    (defparameter *table*
      (~> *test-data*
          cl-ds:whole-range
          vellum:decorate
          (vellum:to-table :header (vellum:header)))))

  (prove:is (vellum:at *table* 0 0) 1)
  (prove:is (vellum:at *table* 1 0) 2)
  (prove:is (vellum:at *table* 2 0) 3)

  (prove:is (vellum:at *table* 0 1) 'a)
  (prove:is (vellum:at *table* 1 1) 'b)
  (prove:is (vellum:at *table* 2 1) 'c)

  (prove:is (vellum:at *table* 0 2) 5)
  (prove:is (vellum:at *table* 1 2) 6)
  (prove:is (vellum:at *table* 2 2) 7)

  (prove:is (vellum:at *table* 0 3) 's)
  (prove:is (vellum:at *table* 1 3) 's)
  (prove:is (vellum:at *table* 2 3) 's)

  (defparameter *replica*
    (vellum:transform *table*
                     (vellum:body ()
                       (setf (vellum:rr 0) (+ 1 (vellum:rr 0))))
                     :in-place nil))

  (prove:is (vellum:at *table* 0 0) 1)
  (prove:is (vellum:at *table* 1 0) 2)
  (prove:is (vellum:at *table* 2 0) 3)

  (prove:is (vellum:at *replica* 0 0) 2)
  (prove:is (vellum:at *replica* 1 0) 3)
  (prove:is (vellum:at *replica* 2 0) 4)

  (vellum:transform *table*
                   (vellum:body ()
                     (setf (vellum:rr 0) (* 2 (vellum:rr 0))))
                   :in-place t)

  (prove:is (vellum:at *table* 0 0) 2)
  (prove:is (vellum:at *table* 1 0) 4)
  (prove:is (vellum:at *table* 2 0) 6)

  (prove:is (vellum:at *replica* 0 0) 2)
  (prove:is (vellum:at *replica* 1 0) 3)
  (prove:is (vellum:at *replica* 2 0) 4)

  (defparameter *concatenated-table* (vellum:vstack *table*
                                                   (list *replica*)))
  (prove:is (column-count *concatenated-table*) 4)
  (prove:is (row-count *concatenated-table*) 6)

  (prove:is (vellum:at *concatenated-table* 0 0) 2)
  (prove:is (vellum:at *concatenated-table* 1 0) 4)
  (prove:is (vellum:at *concatenated-table* 2 0) 6)
  (prove:is (vellum:at *concatenated-table* 3 0) 2)
  (prove:is (vellum:at *concatenated-table* 4 0) 3)
  (prove:is (vellum:at *concatenated-table* 5 0) 4)

  (defparameter *sub-table* (select *concatenated-table*
                              :rows '(:take-from 1 :take-to 3)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 1 0) 6)
  (prove:is (at *sub-table* 2 0) 2)

  (defparameter *sub-table* (select *concatenated-table*
                              :columns '(:take-to 2)))
  (prove:is (column-count *sub-table*) 3)
  (prove:is (row-count *sub-table*) 6)

  (defparameter *sub-table* (select *concatenated-table*
                              :rows '(:v 1 :v 2 :v 3)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 1 0) 6)
  (prove:is (at *sub-table* 2 0) 2))

(prove:finalize)
