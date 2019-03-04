(in-package #:cl-df.table)


(cl-ds.alg.meta:define-aggregation-function
    to-table to-table-function

  (:range &key key class)

  (:range &key
          (key #'identity)
          (class 'standard-table))

  (%iterator %columns)

  ((&rest all)
   cl-ds.utils:todo)

  ((row)
   cl-ds.utils:todo)

  (cl-ds.utils:todo))
