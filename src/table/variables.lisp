(cl:in-package #:vellum.table)


(defvar *table* nil)

(defvar *transform-in-place* nil)

(defvar *transform-control*
  (lambda (operation)
    (error 'cl-ds:operation-not-allowed
           :format-control "Operation ~a is not supported in the current dynamic environment."
           :format-arguments (list operation))))


(defvar *current-row*)
