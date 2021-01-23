(cl:in-package #:vellum.header)


(defun unique-names (headers)
  (iterate
    (with result = (make-hash-table :test 'equal))
    (for offset in (~>> (cl-ds.utils:scan #'+ headers :key #'column-count
                                                      :initial-value 0)
                        (cons 0)))
    (for header in headers)
    (iterate
      (for (name value) in-hashtable (read-column-names header))
      (unless (null (shiftf (gethash name result) (+ offset value)))
        (error 'name-duplicated
               :value name
               :format-arguments (list name))))
    (finally (return result))))
