(cl:in-package #:vellum.header)


(define-condition no-column (cl-ds:argument-value-not-in-allowed-set)
  ()
  (:default-initargs :format-control "No column ~a."))


(define-condition no-row (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "No active row."))


(define-condition no-header (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "No active header."))


(define-condition invalid-name (cl-ds:invalid-value)
  ())


(define-condition name-duplicated (cl-ds:invalid-value)
  ()
  (:default-initargs :format-control "Detected name ~a duplication."))


(define-condition headers-incompatible (cl-ds:operation-not-allowed)
  ((%headers :initarg :headers))
  (:default-initargs :format-control "Headers are incompatible."))


(define-condition unable-to-construct-row (cl-ds:textual-error)
  ((%header :initarg :header))
  (:default-initargs :format-control "Can't create row from data."))


(define-condition invalid-input-for-row (cl-ds:invalid-value)
  ())
