(in-package #:cl-df)


(defvar *transform-in-place* nil)

(defgeneric hstack (frame &rest more-frames))

(defgeneric vstack (frame &rest more-frames))

(defgeneric hslice (frame selector))

(defgeneric vslice (frame selector))

(defgeneric frame-at (frame column row))

(defgeneric (setf frame-at) (new-value frame column row))

(defgeneric transform (frame function &key (in-place)))

(defgeneric row-delete (row))

(defgeneric row-at (row column))

(defgeneric (setf row-at) (row column))

(defgeneric column-count (frame))

(defgeneric (setf column-count) (new-value frame))

(defgeneric row-count (frame))

(defgeneric (setf row-count) (new-value frame))
