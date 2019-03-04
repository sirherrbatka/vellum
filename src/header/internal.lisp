(in-package #:cl-data-frames.header)


(defun unique-aliases (headers)
  (iterate
    (with result = (make-hash-table))
    (for offset in (~>> (cl-ds.utils:scan #'+ headers :key #'column-count
                                                      :initial-value 0)
                        (cons 0)))
    (for header in headers)
    (iterate
      (for (alias value) in-hashtable (read-column-aliases header))
      (unless (null (shiftf (gethash alias result) (+ offset value)))
        cl-ds.utils:todo))
    (finally (return result))))
