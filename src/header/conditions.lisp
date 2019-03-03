(in-package #:cl-data-frames.header)


(define-condition no-column (cl-ds:not-in-allowed-set)
  ())


(define-condition no-row (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :text "No active row."))


(define-condition no-header (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :text "No active header."))
