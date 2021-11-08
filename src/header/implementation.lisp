(cl:in-package #:vellum.header)


(defmethod column-type ((header standard-header)
                        column)
  (~>> (column-signature header column)
       read-type))


(defmethod column-count ((header standard-header))
  (~> header read-column-signatures length))


(defmethod cl-ds:consume-front ((range frame-range-mixin))
  (if (null *header*)
      (restart-case
          (bind (((:values data more) (call-next-method)))
            (if (no more)
                (values nil nil)
                (let ((row (make-row range data)))
                  (set-row row)
                  (values row t))))
        (skip-row () (cl-ds:consume-front range)))
      (call-next-method)))


(defmethod cl-ds:peek-front ((range frame-range-mixin))
  (if (null *header*)
      (bind (((:values data more) (call-next-method)))
        (if (no more)
            (values nil nil)
            (let ((row (make-row range data)))
              (set-row row)
              (values row t))))
      (call-next-method)))


(defmethod cl-ds:traverse ((range frame-range-mixin) function)
  (if (null *header*)
      (let* ((header (read-header range))
             (bind-row-closure (bind-row-closure function
                                                 :header header)))
        (with-header (header)
          (call-next-method
           range (lambda (data)
                   (restart-case (let ((row (make-row range data)))
                                   (set-row row)
                                   (funcall bind-row-closure row))
                     (skip-row ()
                       :report "Skip this row."
                       nil))))))
      (call-next-method)))


(defmethod cl-ds:across ((range frame-range-mixin) function)
  (if (null *header*)
      (let* ((header (read-header range))
             (bind-row-closure (bind-row-closure function
                                                 :header header)))
        (with-header (header)
          (call-next-method
           range (lambda (data)
                   (restart-case (let ((row (make-row range data)))
                                   (set-row row)
                                   (funcall bind-row-closure row))
                     (skip-row ()
                       :report "Skip this row."
                       nil))))))
    (call-next-method)))


(defmethod make-row ((range frame-range-mixin)
                     (data vector))
  (iterate
    (with header = (read-header range))
    (with result = (~> header column-count
                       (make-array :initial-element :null)))
    (for elt in-vector data)
    (for i from 0)
    (check-predicate header i elt)
    (setf (aref result i) elt)
    (finally (return result))))


(more-conditions:define-condition-translating-method
    make-row (range data)
  ((error unable-to-construct-row)
   :header (read-header range)))


(defun validate-row (row data)
  (unless (= (length (the simple-vector row)) (length data))
    (error 'invalid-input-for-row
           :format-control "Desired number of columns is not equal to number of columns in the data."
           :value data))
  row)


(defmethod make-row ((range frame-range-mixin)
                     (data list))
  (iterate
    (with header = (header))
    (with result = (~> header column-count
                       (make-array :initial-element nil)
                       (validate-row data)))
    (for elt in data)
    (for i from 0)
    (setf (aref result i) elt)
    (finally (return result))))


(defmethod concatenate-headers ((header standard-header)
                                &rest more-headers)
  (push header more-headers)
  (let* ((names (unique-names more-headers))
         (signatures (apply #'concatenate 'vector
                            (mapcar #'read-column-signatures more-headers))))
    (make-standard-header :column-names names
                          :column-signatures signatures)))


(defmethod select-columns ((header standard-header)
                           columns)
  (bind ((selected (~> columns
                       (cl-ds.alg:on-each (extracting-signature header))
                       cl-ds.alg:to-vector))
         (names (make-hash-table :size (length selected)
                                 :test 'equal)))
    (declare (type vector selected))
    (iterate
      (declare (ignorable id))
      (for i from 0)
      (for (original new id) in-vector selected)
      (check-column-signatures-compatibility original new)
      (for name = (read-name new))
      (when (null name) (next-iteration))
      (when (symbolp name)
        (setf name (symbol-name name)))
      (unless (null (shiftf (gethash name names) i))
        (error 'name-duplicated
               :format-arguments (list name)
               :value name)))
    (values (make-standard-header :column-signatures (map 'vector #'second selected)
                                  :column-names names)
            (map 'vector #'third selected))))


(defmethod alter-columns ((header standard-header)
                          columns)
  (bind ((altered (~> columns
                      (cl-ds.alg:on-each (extracting-signature header))
                      cl-ds.alg:to-vector))
         (altered-table (cl-ds.alg:to-hash-table
                         altered
                         :test 'equal
                         :hash-table-key (compose #'read-name
                                                  #'first)))
         (names (make-hash-table :test 'equal))
         (column-count (column-count header))
         (new-columns (make-array column-count)))
    (iterate
      (declare (ignorable id))
      (for i from 0 below column-count)
      (for signature = (column-signature header i))
      (for column-name = (read-name signature))
      (for (values change found) = (gethash column-name altered-table))
      (unless found
        (setf (aref new-columns i) signature)
        (unless (null (shiftf (gethash column-name names) i))
          (error 'name-duplicated
                 :format-arguments (list column-name)
                 :value column-name))
        (next-iteration))
      (for (original new id) = change)
      (check-column-signatures-compatibility original new)
      (for name = (read-name new))
      (setf (aref new-columns i) new)
      (unless (null (shiftf (gethash name names) i))
        (error 'name-duplicated
               :format-arguments (list name)
               :value name)))
    (values (make-standard-header :column-signatures new-columns
                                  :column-names names)
            (map 'vector #'third altered))))


(defmethod column-specs ((header standard-header))
  (iterate
    (for signature in-vector (read-column-signatures header))
    (collect (column-signature-spec signature))))


(defmethod check-column-signatures-compatibility
    ((first-signature column-signature)
     (second-signature column-signature))
  (unless (equal (read-type first-signature)
                 (read-type second-signature))
    (error 'cl-ds:operation-not-allowed
           :format-control "Attempted to change type of the signature from ~a to ~a."
           :format-arguments (list (read-type first-signature)
                                   (read-type second-signature)))))


(defmethod column-signature-spec ((signature column-signature))
  (list :name (read-name signature)
        :type (read-type signature)))
