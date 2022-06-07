(cl:in-package #:vellum.table)


(define-condition no-transformation (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "Not performing transformation right now."))


(define-condition transformation-error (cl-ds:textual-error)
  ()
  (:default-initargs :format-control "Error during transformation."))


(define-condition aggregation-enforced-and-not-found (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "AGGREGATED-OUTPUT was :ENFORCE, but no AGGREGATION was found in the BIND-ROW"))


(define-condition aggregation-required-and-not-found (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "AGGREGATED-OUTPUT was :REQUIRE, but no AGGREGATION was found in the BIND-ROW"))


(define-condition aggregation-prohibited-and-found (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "AGGREGATED-OUTPUT was :PROHIBIT, but AGGREGATION was found in the BIND-ROW"))
