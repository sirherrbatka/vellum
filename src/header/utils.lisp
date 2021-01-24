(cl:in-package #:vellum.header)


(defun read-new-value ()
  (format t "Enter a new value: ")
  (multiple-value-list (eval (read))))
