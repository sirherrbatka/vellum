(cl:in-package #:vellum.header)


(defun constantly-t (&rest ignored)
  (declare (ignore ignored))
  t)


(defstruct column-signature
  (type t)
  (predicate 'constantly-t)
  (name nil :type (or symbol string)))


(defun read-predicate (column-signature)
  (column-signature-predicate column-signature))


(defun read-name (column-signature)
  (column-signature-name column-signature))


(defun read-type (column-signature)
  (column-signature-type column-signature))


(defstruct standard-header
  column-signatures
  (column-names (make-hash-table :test 'equal) :type hash-table))


(defun read-column-signatures (header)
  (standard-header-column-signatures header))


(defun read-column-names (header)
  (standard-header-column-names header))


(defmethod cl-ds.utils:cloning-information append
    ((header standard-header))
  '((:column-signatures standard-header-column-signatures)
    (:column-names standard-header-column-names)))


(defmethod cl-ds.utils:cloning-information append
    ((signature column-signature))
  '((:type read-type)
    (:name read-name)
    (:predicate read-predicate)))


(defclass frame-range-mixin ()
  ((%header :initarg :header
            :reader read-header)))


(defmethod cl-ds.utils:cloning-information append
    ((range frame-range-mixin))
  '((:header read-header)))
