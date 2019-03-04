(in-package #:cl-df.table)


(defvar *transform-in-place* nil)

(defgeneric hstack (frame &rest more-frames))

(defgeneric vstack (frame &rest more-frames))

(defgeneric hslice (frame selector))

(defgeneric vslice (frame selector))

(defgeneric at (frame column row))

(defgeneric (setf at) (new-value frame column row))

(defgeneric transform (frame function &key in-place start end))

(defgeneric row-erase (row))

(defgeneric column-count (frame))

(defgeneric row-count (frame))

(defgeneric (setf row-count) (new-value frame))

(defgeneric column-name (frame column))

(defgeneric header (frame))
