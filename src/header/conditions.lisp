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
  ((%value :initarg :value
           :reader value)
   (%column-number :initarg :column-number
                   :reader column-number))
  (:default-initargs :text "Predicate for value in the colum returned nil.")
  (:report (lambda (condition stream)
             (format stream "~a The value is: ~a. The column number is ~a.~%"
                     (cl-ds:read-text condition)
                     (value condition)
                     (column-number condition)))))


(define-condition conversion-failed (cl-ds:operation-not-allowed)
  ((%value :initarg :value
           :reader value)
   (%target-type :initarg :target-type
                 :reader target-type))
  (:default-initargs :text "Can't convert value.")
  (:report (lambda (condition stream)
             (format stream "~a The value is: ~a. The column type is ~a.~%"
                     (cl-ds:read-text condition)
                     (value condition)
                     (target-type condition)))))
