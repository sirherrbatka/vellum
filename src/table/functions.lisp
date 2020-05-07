(in-package #:vellum.table)


(defun finish-transformation ()
  (funcall *transform-control* :finish))


(defun nullify ()
  (funcall *transform-control* :nullify))


(defun drop-row ()
  (funcall *transform-control* :drop))
