(cl:in-package #:vellum.header)


(defstruct (column-signature (:constructor make-column-signature-impl))
  (type t)
  (name nil :type (or symbol string)))


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


(defclass frame-range-mixin ()
  ((%header :initarg :header
            :reader read-header)))


(defmethod cl-ds.utils:cloning-information append
    ((range frame-range-mixin))
  '((:header read-header)))
