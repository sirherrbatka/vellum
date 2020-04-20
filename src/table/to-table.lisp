(in-package #:vellum.table)


(cl-ds.alg.meta:define-aggregation-function
    to-table to-table-function

    (:range &key body key class header-class columns header)

    (:range &key
     (key #'identity)
     (body nil)
     (class 'standard-table)
     (header-class 'vellum.header:standard-header)
     (columns '())
     (header (apply #'vellum.header:make-header
                    header-class columns)))

    (%iterator %row %body %class %columns %column-count %header)

    ((setf %header header
           %class class
           %body body
           %column-count (vellum.header:column-count %header)
           %columns (make-array %column-count))
     (iterate
       (for i from 0 below %column-count)
       (setf (aref %columns i)
             (vellum.column:make-sparse-material-column
              :element-type (vellum.header:column-type %header i))))
     (setf %iterator (vellum.column:make-iterator %columns)
           %row (make 'vellum.table:setfable-table-row
                      :iterator %iterator)))

    ((row)
     (iterate
       (for i from 0 below %column-count)
       (setf (vellum.column:iterator-at %iterator i)
             (vellum.header:row-at %header row i)))
     (let ((body %body))
       (unless (null body)
         (vellum.header:with-header (%header)
           (let ((vellum.header:*row* %row))
             (funcall body %row)))))
     (vellum.column:move-iterator %iterator 1))

    ((vellum.column:finish-iterator %iterator)
     (make %class
           :header %header
           :columns %columns)))


(defmethod to-table ((range vellum.header:frame-range-mixin)
                     &key (key #'identity)
                       (class 'vellum.table:standard-table)
                       (header-class 'vellum.header:standard-header)
                       (columns '())
                       (body nil)
                       (header (apply #'vellum.header:make-header
                                      header-class
                                      columns)))
  (vellum.header:with-header (header)
    (call-next-method range :key key
                            :class class
                            :body body
                            :header-class header-class
                            :columns columns
                            :header header)))
