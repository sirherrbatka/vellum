(cl:in-package #:vellum.header)


(declaim (inline validate-active-row))
(defun validate-active-row ()
  (when (null *row*)
    (error 'no-row)))


(declaim (inline validate-active-header))
(defun validate-active-header ()
  (when (null *header*)
    (error 'no-header)))


(declaim (inline set-row))
(defun set-row (row)
  (validate-active-row)
  (setf (unbox *row*) row))


(declaim (inline row))
(defun row ()
  (validate-active-row)
  (unbox *row*))


(declaim (inline header))
(defun header ()
  (validate-active-header)
  *header*)


(declaim (inline decorate))
(defun decorate (range &key list-format (header (header)))
  (check-type list-format (member nil :pair))
  (decorate-data header range :list-format list-format))


(declaim (inline rr))
(defun rr (index &optional (row (row)))
  (row-at (header) row index))


(declaim (inline (setf rr)))
(defun (setf rr) (new-value index &optional (row (row)))
  (setf (row-at (header) row index) new-value))


(defun current-row-as-vector (&optional (header (header)) (row (row)))
  (iterate
    (with column-count = (column-count header))
    (with result = (make-array column-count))
    (for i from 0 below column-count)
    (setf (aref result i) (rr i row))
    (finally (return result))))


(defun make-bind-row (optimized-closure non-optimized-closure)
  (lret ((result (make 'bind-row :optimized-closure optimized-closure)))
    (c2mop:set-funcallable-instance-function result non-optimized-closure)))


(defun ensure-index (header index/name)
  (check-type index/name (or symbol string non-negative-integer))
  (if (numberp index/name)
      index/name
      (vellum.header:name-to-index header
                                   index/name)))
