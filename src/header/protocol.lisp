(cl:in-package #:vellum.header)


(defgeneric name-to-index (header name))

(defgeneric index-to-name (header index))

(defgeneric column-type (header column))

(defgeneric column-signature (header column))

(defgeneric column-predicate (header column))

(defgeneric check-predicate (header column value))

(defgeneric make-header (class &rest columns))

(defgeneric column-count (header))

(defgeneric row-at (header row position))

(defgeneric (setf row-at) (new-value header row position))

(defgeneric make-row (range data))

(defgeneric make-value (header source index))

(defgeneric concatenate-headers (header &rest more-headers))

(defgeneric select-columns (header columns))

(defgeneric column-specs (header))

(defgeneric bind-row-closure (bind-row-object &key header))

(defgeneric check-column-signatures-compatibility
    (old-signature new-signature))
