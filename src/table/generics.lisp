(cl:in-package #:vellum.table)


(defgeneric hstack* (frame more-frames &key isolate))

(defgeneric vstack* (frame more-frames))

(defgeneric select (frame &key columns rows))

(defgeneric vmask (frame mask &key in-place))

(defgeneric at (frame row column))

(defgeneric (setf at) (new-value frame row column))

(defgeneric transform (frame function
                       &key in-place start end restarts-enabled aggregation-output))

(defgeneric iterator (frame in-place))

(defgeneric transformation (frame bind-row &key in-place start restarts-enabled aggregation-output))

(defgeneric transform-row (transformation &optional bind-row-closure))

(defgeneric transformation-result (transformation))

(defgeneric row-erase (row))

(defgeneric column-count (frame))

(defgeneric row-count (frame))

(defgeneric column-name (frame column))

(defgeneric column-type (frame column))

(defgeneric column-at (frame column))

(defgeneric header (frame))

(defgeneric remove-nulls (frame &key in-place))

(defgeneric make-table* (class &optional header))

(defgeneric erase! (frame row column))

(defgeneric show (as table &key &allow-other-keys))

(defgeneric alter-columns (table &rest columns))

(defgeneric bind-row-closure (bind-row-object &key header aggregation-output))

(defgeneric read-iterator (object))

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
