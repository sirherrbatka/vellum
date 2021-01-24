(cl:in-package #:vellum.column)


(defgeneric column-type (column))
(defgeneric column-size (column))
(defgeneric column-at (column index))
(defgeneric (setf column-at) (new-value column index))
(defgeneric make-iterator (columns &key &allow-other-keys))
(defgeneric augment-iterator (iterator column))
(defgeneric finish-iterator (iterator))
(defgeneric remove-nulls (iterator))
(defgeneric truncate-to-length (column length))
