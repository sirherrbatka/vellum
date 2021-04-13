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


(defun make-signature (signature-class c)
  (apply #'make signature-class
         (cond ((listp c) c)
               ((atom c) `(:name ,c)))))


(defun extracting-signature (header)
  (let ((column-signature-class (read-column-signature-class header)))
    (lambda (column)
      (let* ((existing-signature (column-signature header
                                                   (if (listp column)
                                                       (first column)
                                                       column))))
        (if (listp column)
            (let ((new-spec (second column)))
              (list existing-signature
                    (make-signature column-signature-class
                                    (append (cond ((listp new-spec) new-spec)
                                                  ((atom new-spec) (list :name new-spec)))
                                            (column-signature-spec existing-signature)))
                    (first column)))
            (list existing-signature
                  existing-signature
                  column))))))
