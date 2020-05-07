(in-package #:vellum.column)


(define-condition column-error (program-error)
  ())


(define-condition column-type-error (type-error)
  ((%column :initarg :column
            :reader column-type-error-column))
  (:report (lambda (e stream)
             (format stream "Expected type ~a for value ~a in the ~a column."
                     (type-error-expected-type e)
                     (type-error-datum e)
                     (column-type-error-column e)))))


(define-condition iterator-error (program-error)
  ())


(define-condition index-out-of-column-bounds (column-error
                                              cl-ds:argument-value-out-of-bounds)
  ())


(define-condition no-such-column (column-error
                                  cl-ds:argument-value-out-of-bounds)
  ())


(define-condition setting-to-null (column-error
                                   cl-ds:invalid-value)
  ((%column :initarg :column
            :reader column-type-error-column)))
