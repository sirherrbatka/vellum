(in-package #:cl-df.column)


(defparameter *column* (make-sparse-material-column))
(defparameter *iterator* (make-iterator *column*))


(iterate
  (for i from 0 below 256)
  (setf (iterator-at *iterator* 0) i)
  (move-iterator *iterator* 1))
