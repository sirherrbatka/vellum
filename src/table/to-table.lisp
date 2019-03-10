(in-package #:cl-df.table)


(cl-ds.alg.meta:define-aggregation-function
    to-table to-table-function

  (:range &key key class)

  (:range &key
          (key #'identity)
          (class 'standard-table))

  (%iterator %columns %column-count %header)

  ((&key &allow-other-keys)
   (setf %header (cl-df.header:header)
         %column-count (cl-data-frames.header:column-count %header)
         %columns (make-array %column-count))
   (iterate
     (for i from 0 below %column-count)
     (setf (aref %columns i)
           (cl-df.column:make-sparse-material-column
            :element-type (cl-df.header:column-type %header i))))
   (setf %iterator (~> %columns
                       first-elt
                       cl-df.column:make-iterator))
   (iterate
     (for i from 1 below %column-count)
     (for column = (aref %columns i))
     (cl-df.column:augment-iterator %iterator column)))

  ((row)
   (iterate
     (for i from 0 below %column-count)
     (setf (cl-df.column:iterator-at %iterator i)
           (cl-df.header:row-at %header row i))
     (finally (cl-df.column:move-iterator %iterator 1))))

  ((cl-df.column:finish-iterator %iterator)
   (make 'standard-table
         :header %header
         :columns %columns)))
