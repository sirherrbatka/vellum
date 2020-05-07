(cl:in-package #:vellum.selection)


(define-condition alias-when-selecting-row (cl-ds:invalid-value)
  ())


(define-condition selection-syntax-error (cl-ds:textual-error)
  ())
