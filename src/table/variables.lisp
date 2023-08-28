(cl:in-package #:vellum.table)


(defvar *table* nil)

(defvar *transform-in-place* nil)

(defvar *wrap-errors* t)

(defvar *enable-restarts* t)

(defvar *transform-control*
  (lambda (operation &rest arguments)
    (declare (ignore arguments))
    (error 'cl-ds:operation-not-allowed
           :format-control "Operation ~a is not supported in the current dynamic environment."
           :format-arguments (list operation))))


(defvar *current-row*)
