(cl:in-package #:vellum.header)


(defgeneric name-to-index (header name))

(defgeneric index-to-name (header index))

(defgeneric column-type (header column))

(defgeneric column-signature (header column))

(defgeneric column-predicate (header column))

(defgeneric check-predicate (header column value))

(defgeneric column-count (header))

(defgeneric make-row (range data))

(defgeneric make-value (header source index))

(defgeneric concatenate-headers (header &rest more-headers))

(defgeneric select-columns (header columns))

(defgeneric alter-columns (header columns))

(defgeneric column-specs (header))

(defgeneric check-column-signatures-compatibility
    (old-signature new-signature))

(defgeneric column-signature-spec (column-signature))
