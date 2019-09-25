(in-package #:cl-df.table)


(defgeneric hstack (frame more-frames))

(defgeneric vstack (frame more-frames))

(defgeneric hselect (frame selector))

(defgeneric vselect (frame selector))

(defgeneric vmask (frame mask &key in-place))

(defgeneric at (frame column row))

(defgeneric (setf at) (new-value frame column row))

(defgeneric transform (frame function &key in-place))

(defgeneric row-erase (row))

(defgeneric column-count (frame))

(defgeneric row-count (frame))

(defgeneric column-name (frame column))

(defgeneric column-type (frame column))

(defgeneric header (frame))

(defgeneric remove-nulls (frame &key in-place))

(defgeneric make-table (class &optional header))
