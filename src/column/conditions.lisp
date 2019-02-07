(in-package #:cl-df.column)


(define-condition column-error (program-error)
  ())


(define-condition index-out-of-bounds (column-error
                                       cl-ds:argument-out-of-bounds)
  ())
