(in-package #:cl-data-frames.header)


(defgeneric alias-to-index (header alias))

(defgeneric index-to-alias (header index))

(defgeneric column-type (header column))

(defgeneric column-signature (header column))

(defgeneric column-predicate (header column))

(defgeneric make-header (class &rest columns))

(defgeneric column-count (header))

(defgeneric row-at (header row position))

(defgeneric (setf row-at) (new-value header row position))

(defgeneric decorate-data (header data &key &allow-other-keys))

(defgeneric make-row (header range data))

(defgeneric make-value (header source index))

(defgeneric concatenate-headers (header &rest more-headers))

(defgeneric select-columns (header columns))
