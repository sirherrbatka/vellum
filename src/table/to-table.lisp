(in-package #:cl-df.table)


(cl-ds.alg.meta:define-aggregation-function
    to-table to-table-function

  (:range &key key class)

  (:range &key
          (key #'identity)
          (class 'standard-table))

  (%iterator %columns)

  ((&rest all)
   (let* ((header (cl-data-frames.header:header))
          (column-count (cl-data-frames.header:column-count header)))
     (setf %columns (make-array column-count))
     (iterate
       (for i from 0 below column-count)
       (setf (aref %columns i)
             (cl-df.column:make-sparse-material-column
              :element-type (cl-df.header:column-type header i))))
     (setf %iterator (~> %columns
                         first-elt
                         cl-df.column:make-iterator))
     (iterate
       (for i from 1 below column-count)
       (for column = (aref %columns i))
       (cl-df.column:augment-iterator %iterator column))))

  ((row)
   cl-ds.utils:todo)

  (cl-ds.utils:todo))
