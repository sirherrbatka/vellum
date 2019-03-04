(in-package #:cl-data-frames.header)


(defgeneric alias-to-index (header alias))

(defgeneric index-to-alias (header index))

(defgeneric column-type (header column))

(defgeneric column-predicate (header column))

(defgeneric make-header (class &rest columns))

(defgeneric validate-column-specification (class column-specification))

(defgeneric column-count (header))

(defgeneric row-at (header row position))

(defgeneric (setf row-at) (new-value header row position))

(defgeneric decorate-data (header data))

(defgeneric make-row (header range data))

(defgeneric make-value (header source index))

(defgeneric convert (value type))

(defgeneric insert-column-into-header (header index column-specification))

(defgeneric replace-column-in-header (header index column-specification))

(defgeneric remove-column-in-header (header index))

(defgeneric concatenate-headers (header &rest more-headers))
