(in-package #:cl-data-frames.header)


(defgeneric alias-to-index (header alias))

(defgeneric index-to-alias (header index))

(defgeneric column-type (header column))

(defgeneric make-header (class &rest columns))

(defgeneric validate-column-specification (class column-specification))
