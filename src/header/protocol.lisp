(in-package #:cl-data-frames.header)


(defgeneric alias-to-index (header alias))

(defgeneric index-to-alias (header index))

(defgeneric (setf index-to-alias) (new-value header index))

(defgeneric column-type (header column))

(defgeneric (setf column-type) (header column))

(defgeneric make-header (class &rest columns))
