(in-package #:cl-df.table)

(prove:plan 42)

(progn
  (defparameter *test-data* #(#(1 a 5 s)
                              #(2 b 6 s)
                              #(3 c 7 s)))

  (cl-df:with-header ((cl-df:make-header 'cl-df:standard-header
                                         nil nil nil nil))
    (defparameter *table*
      (~> *test-data*
          cl-ds:whole-range
          cl-df:decorate
          cl-df:to-table)))

  (prove:is (cl-df:at *table* 0 0) 1)
  (prove:is (cl-df:at *table* 0 1) 2)
  (prove:is (cl-df:at *table* 0 2) 3)

  (prove:is (cl-df:at *table* 1 0) 'a)
  (prove:is (cl-df:at *table* 1 1) 'b)
  (prove:is (cl-df:at *table* 1 2) 'c)

  (prove:is (cl-df:at *table* 2 0) 5)
  (prove:is (cl-df:at *table* 2 1) 6)
  (prove:is (cl-df:at *table* 2 2) 7)

  (prove:is (cl-df:at *table* 3 0) 's)
  (prove:is (cl-df:at *table* 3 1) 's)
  (prove:is (cl-df:at *table* 3 2) 's)

  (defparameter *replica*
    (cl-df:transform *table*
                     (cl-df:body ()
                       (setf (cl-df:rr 0) (+ 1 (cl-df:rr 0))))
                     :in-place nil))

  (prove:is (cl-df:at *table* 0 0) 1)
  (prove:is (cl-df:at *table* 0 1) 2)
  (prove:is (cl-df:at *table* 0 2) 3)

  (prove:is (cl-df:at *replica* 0 0) 2)
  (prove:is (cl-df:at *replica* 0 1) 3)
  (prove:is (cl-df:at *replica* 0 2) 4)

  (cl-df:transform *table*
                   (cl-df:body ()
                     (setf (cl-df:rr 0) (* 2 (cl-df:rr 0))))
                   :in-place t)

  (prove:is (cl-df:at *table* 0 0) 2)
  (prove:is (cl-df:at *table* 0 1) 4)
  (prove:is (cl-df:at *table* 0 2) 6)

  (prove:is (cl-df:at *replica* 0 0) 2)
  (prove:is (cl-df:at *replica* 0 1) 3)
  (prove:is (cl-df:at *replica* 0 2) 4)

  (defparameter *concatenated-table* (cl-df:vstack *table*
                                                   (list *replica*)))
  (prove:is (column-count *concatenated-table*) 4)
  (prove:is (row-count *concatenated-table*) 6)

  (prove:is (cl-df:at *concatenated-table* 0 0) 2)
  (prove:is (cl-df:at *concatenated-table* 0 1) 4)
  (prove:is (cl-df:at *concatenated-table* 0 2) 6)
  (prove:is (cl-df:at *concatenated-table* 0 3) 2)
  (prove:is (cl-df:at *concatenated-table* 0 4) 3)
  (prove:is (cl-df:at *concatenated-table* 0 5) 4)

  (defparameter *sub-table* (hselect *concatenated-table* (selection 1 4)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 0 1) 6)
  (prove:is (at *sub-table* 0 2) 2)

  (defparameter *sub-table* (hselect *concatenated-table* (iota 3 :start 1)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 0 1) 6)
  (prove:is (at *sub-table* 0 2) 2))

(prove:finalize)
