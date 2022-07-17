(cl:in-package #:vellum.table)


(define-condition no-transformation (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "Not performing transformation right now."))


(define-condition transformation-error (cl-ds:textual-error)
  ()
  (:default-initargs :format-control "Error during transformation.")
  (:documentation "Wrapper for errors produced during transformation, when :WRAP-ERRORS is true."))


(define-condition aggregation-required-and-not-found (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "AGGREGATED-OUTPUT was :REQUIRE, but no AGGREGATION was found in the BIND-ROW")
  (:documentation "Thise error may be signalled only when :REQUIRE is passed as :AGGREGATED-OUTPUT."))


(define-condition aggregation-prohibited-and-found (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "AGGREGATED-OUTPUT was :PROHIBIT, but AGGREGATION was found in the BIND-ROW")
  (:documentation "Thise error may be signalled only when :PROHIBIT is passed as :AGGREGATED-OUTPUT."))
