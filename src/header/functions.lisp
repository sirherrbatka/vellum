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


(defun make-header (&rest columns)
  (declare (optimize (debug 3)))
  (let* ((result (make-standard-header))
         (column-signatures (map 'vector
                                 #'make-signature
                                 columns))
         (length (length column-signatures))
         (names (iterate
                  (with result = (make-hash-table
                                  :test 'equal
                                  :size length))
                  (for i from 0 below length)
                  (for column = (aref column-signatures i))
                  (for name = (read-name column))
                  (when (null name)
                    (next-iteration))
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


(defun index-to-name (header index)
  (check-type index non-negative-integer)
  (let* ((signature (column-signature header index))
         (name (read-name signature)))
    (if (null name)
        (error 'no-column
               :bounds `(< 0 ,(~> header read-column-signatures length))
               :argument 'index
               :value index
               :format-control "No name for column ~a."
               :format-arguments (list index))
        (if (stringp name)
            name
            (symbol-name name)))))


(defun name-to-index (header name)
  (let* ((names (read-column-names header))
         (index (gethash (etypecase name
                           (symbol (symbol-name name))
                           (string name))
                         names)))
    (when (null index)
      (error 'no-column
             :bounds (hash-table-keys names)
             :argument 'name
             :value name
             :format-arguments (list name)))
    index))


(defun column-signature (header name)
  (unless (integerp name)
    (setf name (name-to-index header name)))
  (let* ((column-signatures (read-column-signatures header))
         (length (length column-signatures)))
    (unless (< name length)
      (error 'no-column
             :bounds `(< 0 ,length)
             :argument 'name
             :value name
             :format-arguments (list name)))
    (~> column-signatures (aref name))))


(defun column-type (header column)
  (~>> (column-signature header column)
       read-type))


(defun make-column-signature (&key name (type t))
  (make-column-signature-impl :name (if (null name)
                                        nil
                                        (string-upcase name))
                              :type type))
