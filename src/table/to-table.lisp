(in-package #:cl-df.table)


(cl-ds.alg.meta:define-aggregation-function
    to-table to-table-function

    (:range &key body key class header-class columns header)

    (:range &key
     (key #'identity)
     (body nil)
     (class 'standard-table)
     (header-class 'cl-df.header:standard-header)
     (columns '())
     (header (apply #'cl-df.header:make-header
                    header-class columns)))

    (%iterator %row %body %class %columns %column-count %header)

    ((setf %header header
           %class class
           %body body
           %column-count (cl-data-frames.header:column-count %header)
           %columns (make-array %column-count))
     (iterate
       (for i from 0 below %column-count)
       (setf (aref %columns i)
             (cl-df.column:make-sparse-material-column
              :element-type (cl-df.header:column-type %header i))))
     (setf %iterator (cl-df.column:make-iterator %columns)
           %row (make 'cl-df.table:setfable-table-row
                      :iterator %iterator)))

    ((row)
     (iterate
       (for i from 0 below %column-count)
       (setf (cl-df.column:iterator-at %iterator i)
             (cl-df.header:row-at %header row i)))
     (let ((body %body))
       (unless (null body)
         (cl-df.header:with-header (%header)
           (let ((cl-df.header:*row* %row))
             (funcall body %row)))))
     (cl-df.column:move-iterator %iterator 1))

    ((cl-df.column:finish-iterator %iterator)
     (make %class
           :header %header
           :columns %columns)))


(defmethod to-table ((range cl-df.header:frame-range-mixin)
                     &key (key #'identity)
                       (class 'cl-df.table:standard-table)
                       (header-class 'cl-df.header:standard-header)
                       (columns '())
                       (body nil)
                       (header (apply #'cl-df.header:make-header
                                      header-class
                                      columns)))
  (cl-df.header:with-header (header)
    (call-next-method range :key key
                            :class class
                            :body body
                            :header-class header-class
                            :columns columns
                            :header header)))
