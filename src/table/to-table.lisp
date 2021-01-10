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

    (%function %transformation)

    ((setf %function (vellum.header:bind-row-closure
                      body :header header)
           %transformation (transformation
                            (make class
                                  :header header
                                  :columns (iterate
                                             (with column-count = (vellum.header:column-count header))
                                             (with result = (make-array column-count))
                                             (for i from 0 below column-count)
                                             (setf (aref result i)
                                                   (vellum.column:make-sparse-material-column
                                                    :element-type (vellum.header:column-type header i)))
                                             (finally (return result))))
                            nil
                            :in-place t)))

    ((row)
     (transform-row %transformation
                    (lambda ()
                      (iterate
                        (for i from 0 below (length row))
                        (for value = (elt row i))
                        (setf (vellum.header:rr i) value))
                      (funcall %function))))

    ((transformation-result %transformation)))


(defmethod to-table ((range vellum.header:frame-range-mixin)
                     &key
                       (class 'vellum.table:standard-table)
                       (body nil)
                       &allow-other-keys)
  (let* ((header (vellum.header:read-header range))
         (function (vellum.header:bind-row-closure body))
         (transformation (transformation
                          (make class
                                :header header
                                :columns (iterate
                                           (with column-count = (vellum.header:column-count header))
                                           (with result = (make-array column-count))
                                           (for i from 0 below column-count)
                                           (setf (aref result i)
                                                 (vellum.column:make-sparse-material-column
                                                  :element-type (vellum.header:column-type header i)))
                                           (finally (return result))))
                         nil
                         :in-place t)))
    (cl-ds:across range
                  (lambda (row &aux (vellum.header:*validate-predicates* t))
                    (transform-row transformation
                                   (lambda ()
                                     (iterate
                                       (for i from 0 below (length row))
                                       (for value = (aref row i))
                                       (setf (vellum.header:rr i) value))
                                     (funcall function)))))
    (transformation-result transformation)))


(defmethod to-table ((input sequence)
                     &key (key #'identity)
                       (class 'vellum.table:standard-table)
                       (header-class 'vellum.header:standard-header)
                       (columns '())
                       (body nil)
                       (header (apply #'vellum.header:make-header
                                      header-class
                                      columns)))
  (to-table (cl-ds:whole-range input)
            :key key
            :class class
            :body body
            :header header))


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
         (function (vellum.header:bind-row-closure body :header header))
         (table (make class
                      :header header
                      :columns columns)))
    (iterate
      (for i from 0 below number-of-columns)
      (setf (aref columns i)
            (vellum.column:make-sparse-material-column
             :element-type (vellum.header:column-type header i))))
    (transform table
               (vellum.header:bind-row ()
                 (unless (< *current-row* (array-dimension input 0))
                   (finish-transformation))
                 (iterate
                   (for i from 0 below number-of-columns)
                   (setf (vellum.header:rr i) (funcall key (aref input *current-row* i))))
                 (funcall function))
               :end nil
               :in-place t)))
