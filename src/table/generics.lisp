(in-package #:cl-df.table)


(defvar *transform-in-place* nil)

(defvar *row*)

(defgeneric hstack (frame &rest more-frames))

(defgeneric vstack (frame &rest more-frames))

(defgeneric hslice (frame selector))

(defgeneric vslice (frame selector))

(defgeneric at (frame column row))

(defgeneric (setf frame-at) (new-value frame column row))

(defgeneric transform (frame function &key in-place start end))

(defgeneric row-erase (row))

(defgeneric row-at (row column))

(defgeneric (setf row-at) (row column))

(defgeneric column-count (frame))

(defgeneric (setf column-count) (new-value frame))

(defgeneric row-count (frame))

(defgeneric (setf row-count) (new-value frame))

(defgeneric column-at (frame column))

(defgeneric (setf column-at) (frame column))

(defgeneric column-name (frame column))
