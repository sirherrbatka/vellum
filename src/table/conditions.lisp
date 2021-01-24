(cl:in-package #:vellum.table)


(define-condition no-transformation (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "Not performing transformation right now."))


(define-condition transformation-error (cl-ds:textual-error)
  ()
  (:default-initargs :format-control "Error during transformation."))
