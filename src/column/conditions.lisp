(in-package #:cl-df.column)


(define-condition column-error (program-error)
  ())


(define-condition iterator-error (program-error)
  ())


(define-condition index-out-of-column-bounds (column-error
                                              cl-ds:argument-out-of-bounds)
  ())


(define-condition no-such-column (column-error
                                  cl-ds:argument-out-of-bounds)
  ())


(define-condition iterator-reached-end (iterator-error
                                        cl-ds:textual-error)
  ())
