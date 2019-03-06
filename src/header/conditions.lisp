(in-package #:cl-data-frames.header)


(define-condition no-column (cl-ds:argument-not-in-allowed-set)
  ()
  (:default-initargs :format-control "No column ~a."))


(define-condition no-row (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "No active row."))


(define-condition no-header (cl-ds:operation-not-allowed)
  ()
  (:default-initargs :format-control "No active header."))


(define-condition predicate-failed (cl-ds:operation-not-allowed)
  ((%value :initarg :value
           :reader value)
   (%column-number :initarg :column-number
                   :reader column-number))
  (:default-initargs :format-control "Predicate for ~a in the ~a returned nil."))


(define-condition conversion-failed (cl-ds:operation-not-allowed)
  ((%value :initarg :value
           :reader value)
   (%target-type :initarg :target-type
                 :reader target-type))
  (:default-initargs :format-control "Can't convert ~a to type ~a."))


(define-condition alias-duplicated (cl-ds:operation-not-allowed)
  ((%alias :initarg :alias
           :reader alias))
  (:default-initargs :format-control "Detected alias ~a duplication."))
