(cl:in-package #:cl-df.csv)


(defun clear-buffers (output)
  (iterate
    (for b in-vector output)
    (setf (fill-pointer b) 0)))
