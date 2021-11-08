(cl:in-package #:vellum.header)


(declaim (inline validate-active-row))
(defun validate-active-row ()
  (when (null *row*)
    (error 'no-row)))


(declaim (inline validate-active-header))
(defun validate-active-header ()
  (when (null *header*)
    (error 'no-header)))


(declaim (inline set-row))
(defun set-row (row)
  (validate-active-row)
  (setf (unbox *row*) row))


(declaim (inline row))
(defun row ()
  (validate-active-row)
  (unbox *row*))


(declaim (inline header))
(defun header ()
  (validate-active-header)
  *header*)


(defun ensure-index (header index/name)
  (check-type index/name (or symbol string non-negative-integer))
  (if (numberp index/name)
      (let ((column-count (column-count header)))
        (unless (< index/name column-count)
          (error 'no-column
                 :bounds `(< 0 ,column-count)
                 :argument 'index/name
                 :value index/name
                 :format-arguments (list index/name)))
        index/name)
      (vellum.header:name-to-index header
                                   index/name)))


(defun read-new-value ()
  (format t "Enter a new value: ")
  (multiple-value-list (eval (read))))


(defun column-names (header)
  (~>> header
       vellum.header:column-specs
       (mapcar (lambda (x) (getf x :name)))))


(declaim (inline setf-predicate-check))
(defun setf-predicate-check (new-value header column)
  (tagbody main
     (block nil
       (restart-case (check-predicate header column new-value)
         (keep-old-value ()
           :report "Skip assigning the new value."
           (return (values nil nil)))
         (set-to-null ()
           :report "Set the row position to :null."
           (setf new-value :null)
           (go main))
         (provide-new-value (v)
           :report "Enter the new value."
           :interactive read-new-value
           (setf new-value v)
           (go main)))))
  (values new-value t))


(defun make-header (&rest columns)
  (let* ((result (make-standard-header))
         (column-signatures (map 'vector
                                 (curry #'make-signature 'column-signature)
                                 columns))
         (length (length column-signatures))
         (names (iterate
                  (with result = (make-hash-table
                                  :test 'equal
                                  :size length))
                  (for i from 0 below length)
                  (for column = (aref column-signatures i))
                  (for name = (read-name column))
                  (when (null name) (next-iteration))
                  (when (symbolp name)
                    (setf name (symbol-name name)))
                  (check-type name string)
                  (unless (null (shiftf (gethash name result) i))
                    (error 'name-duplicated
                           :format-arguments (list name)
                           :value name))
                  (finally (return result)))))
    (setf (standard-header-column-signatures result) column-signatures
          (standard-header-column-names result) names)
    result))
