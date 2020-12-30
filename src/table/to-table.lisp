(cl:in-package #:vellum.table)


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
           (let ((vellum.header:*row* (box %row)))
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


(defmethod to-table ((input array)
                     &key (class 'vellum.table:standard-table)
                       (key #'identity)
                       (header-class 'vellum.header:standard-header)
                       (columns '())
                       (body nil)
                       (header (apply #'vellum.header:make-header
                                      header-class
                                      columns)))
  (unless (= 2 (array-rank input))
    (error 'cl-ds:invalid-argument-value
           :argument 'range
           :value input
           :format-control "TO-TABLE works only on 2 dimensional arrays."))
  (let* ((number-of-columns (length columns))
         (columns (make-array (length columns)))
         (function (if body
                       (vellum:bind-row-closure body :header header)
                       (constantly nil)))
         (table (make class
                      :header header
                      :columns columns)))
    (iterate
      (for i from 0 below number-of-columns)
      (setf (aref columns i)
            (vellum.column:make-sparse-material-column
             :element-type (vellum.header:column-type header i))))
    (transform table
               (vellum:bind-row ()
                 (unless (< *current-row* (array-dimension input 0))
                   (finish-transformation))
                 (iterate
                   (for i from 0 below number-of-columns)
                   (setf (vellum.header:rr i) (funcall key (aref input *current-row* i))))
                 (funcall function))
               :end nil
               :in-place t)))
