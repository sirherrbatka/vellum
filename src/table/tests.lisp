(in-package #:cl-df.table)

(prove:plan 3)

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

(prove:finalize)
