(in-package #:cl-data-frames.header)


(define-condition no-column (cl-ds:not-in-allowed-set)
  ())


(define-condition no-row (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :text "No active row."))


(define-condition no-header (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :text "No active header."))


(define-condition predicate-failed (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :text "Predicate for value in the colum returned nil."))
