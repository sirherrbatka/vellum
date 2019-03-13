(in-package #:cl-df.table)


(defvar *transform-in-place* nil)

(defgeneric hstack (frame &rest more-frames))

(defgeneric vstack (frame &rest more-frames))

(defgeneric hslice (frame selector))

(defgeneric vslice (frame selector))

(defgeneric hmask (frame mask &key in-place))

(defgeneric at (frame column row))

(defgeneric (setf at) (new-value frame column row))

(defgeneric transform (frame function &key in-place))

(defgeneric row-erase (row))

(defgeneric column-count (frame))

(defgeneric row-count (frame))

(defgeneric column-name (frame column))

(defgeneric column-type (frame column))

(defgeneric header (frame))
