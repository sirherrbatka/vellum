(cl:in-package #:vellum.table)


(cl-ds.alg.meta:define-aggregation-function
    to-table to-table-function

    (:range &key body key class columns header restarts-enabled)

    (:range &key
     (key #'identity)
     (body nil)
     (class 'standard-table)
     (restarts-enabled t)
     (columns '())
     (header (apply #'vellum.header:make-header columns)))

    (%function %transformation %done)

    ((setf %function (bind-row-closure
                      body :header header)
           %done nil
           %transformation (~> (table-from-header class header)
                               (transformation nil :in-place t
                                                   :restarts-enabled restarts-enabled))))

    ((row)
     (unless %done
       (block main
         (let ((transform-control (lambda (operation)
                                      (cond ((eq operation :finish)
                                             (setf %done t)
                                             (return-from main))
                                            ((eq operation :drop)
                                             (iterate
                                               (for i from 0 below (length row))
                                               (setf (rr i) :null))
                                             (return-from main))
                                            (t nil)))))
           (transform-row %transformation
                          (lambda (&rest all) (declare (ignore all))
                            (iterate
                              (for i from 0 below (length row))
                              (for value = (elt row i))
                              (setf (rr i) value))
                            (let ((*transform-control* transform-control))
                              (funcall %function (standard-transformation-row %transformation)))))))))

    ((transformation-result %transformation)))


(defmethod to-table ((range vellum.header:frame-range-mixin)
                     &key
                       (class 'vellum.table:standard-table)
                       (body nil)
                       (restarts-enabled t)
                       &allow-other-keys)
  (let* ((header (vellum.header:read-header range))
         (function (ensure-function (bind-row-closure body :header header)))
         (transformation (~> (table-from-header class header)
                             (transformation nil :in-place t
                                             :restarts-enabled restarts-enabled)))
         (column-count (vellum.header:column-count header))
         (prev-control (ensure-function *transform-control*))
         (table (standard-transformation-table transformation)))
    (declare (type vellum.header:standard-header header))
    (block main
      (cl-ds:across
       range
       (lambda (row)
         (block function
           (transform-row-impl
            transformation
            (lambda (existing-row)
              (declare (type setfable-table-row existing-row)
                       (type simple-vector row)
                       (optimize (speed 3) (safety 0)))
              (iterate
                (declare (type fixnum i))
                (for i from 0 below (length row))
                (for value = (aref row i))
                (setf (rr i existing-row header) value))
              (when body
                (with-table (table)
                  (let ((row (standard-transformation-row transformation)))
                    (vellum.header:set-row row)
                    (let ((*transform-control* (lambda (operation)
                                                 (cond
                                                   ((eq operation :finish)
                                                    (return-from main))
                                                   ((eq operation :drop)
                                                    (iterate
                                                      (declare (type fixnum i))
                                                      (for i from 0 below column-count)
                                                      (setf (rr i row header) :null))
                                                    (return-from function))
                                                   (t (funcall prev-control operation))))))
                      (funcall function (standard-transformation-row transformation))))))))))))
    (transformation-result transformation)))


(defmethod to-table ((input sequence)
                     &key (key #'identity)
                       (class 'vellum.table:standard-table)
                       (columns '())
                       (body nil)
                       (restarts-enabled t)
                       (header (apply #'vellum.header:make-header columns)))
  (to-table (cl-ds:whole-range input)
            :key key
            :class class
            :restarts-enabled restarts-enabled
            :body body
            :header header))


(defmethod to-table ((input array)
                     &key (class 'vellum.table:standard-table)
                       (key #'identity)
                       (columns '())
                       (body nil)
                       (restarts-enabled t)
                       (header (apply #'vellum.header:make-header
                                      columns)))
  (unless (= 2 (array-rank input))
    (error 'cl-ds:invalid-argument-value
           :argument 'range
           :value input
           :format-control "TO-TABLE works only on 2 dimensional arrays."))
  (let* ((number-of-columns (length columns))
         (columns (make-array (length columns)))
         (function (bind-row-closure body :header header))
         (table (make class
                      :header header
                      :columns columns)))
    (iterate
      (for i from 0 below number-of-columns)
      (setf (svref columns i)
            (vellum.column:make-sparse-material-column
             :element-type (vellum.header:column-type header i))))
    (transform table
               (bind-row ()
                 (unless (< *current-row* (array-dimension input 0))
                   (finish-transformation))
                 (iterate
                   (for i from 0 below number-of-columns)
                   (setf (rr i) (funcall key (aref input *current-row* i))))
                 (funcall function))
               :end nil
               :restarts-enabled restarts-enabled
               :in-place t)))
