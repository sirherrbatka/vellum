(cl:in-package #:vellum.header)


(defmethod name-to-index ((header standard-header)
                          (name string))
  (let* ((names (read-column-names header))
         (index (gethash name names)))
    (when (null index)
      (error 'no-column
             :bounds (hash-table-keys names)
             :argument 'name
             :value name
             :format-arguments (list name)))
    index))


(defmethod name-to-index ((header standard-header)
                          (name symbol))
  (name-to-index header (symbol-name name)))


(defmethod column-signature ((header standard-header)
                             (name symbol))
  (column-signature header (name-to-index header name)))


(defmethod initialize-instance :after ((object column-signature)
                                       &key &allow-other-keys)
  (bind (((:slots %type %predicate %name) object))
    (check-type %type (or list symbol))
    (check-type %name (or symbol string))
    (check-type %predicate (or symbol function))
    (ensure-function %predicate)))


(defmethod column-signature ((header standard-header)
                             (index integer))
  (check-type index non-negative-integer)
  (let* ((column-signatures (read-column-signatures header))
         (length (length column-signatures)))
    (unless (< index length)
      (error 'no-column
             :bounds `(< 0 ,length)
             :argument 'index
             :value index
             :format-arguments (list index)))
    (~> column-signatures (aref index))))


(defmethod index-to-name ((header standard-header)
                          (index integer))
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
        name)))


(defmethod make-header (class &rest columns)
  (let* ((result (make class))
         (signature-class (read-column-signature-class result))
         (column-signatures (map 'vector
                                 (lambda (c)
                                   (apply #'make signature-class
                                          (cond ((listp c) c)
                                                ((atom c) `(:name ,c)))))
                                 columns))
         (names (iterate
                  (with result = (make-hash-table
                                  :test 'equal
                                  :size (length column-signatures)))
                  (for column in-vector column-signatures)
                  (for i from 0)
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
    (setf (slot-value result '%column-signatures) column-signatures
          (slot-value result '%column-names) names)
    result))


(defmethod column-type ((header standard-header)
                        column)
  (~>> (column-signature header column)
       read-type))


(defmethod column-predicate ((header standard-header)
                             column)
  (~>> (column-signature header column)
       read-predicate))


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
                   (let ((row (make-row range data)))
                     (set-row row)
                     (funcall bind-row-closure row))))))
      (call-next-method)))


(defmethod cl-ds:across ((range frame-range-mixin) function)
  (if (null *header*)
      (let* ((header (read-header range))
             (bind-row-closure (bind-row-closure function
                                                 :header header)))
        (with-header (header)
          (call-next-method
           range (lambda (data)
                   (let ((row (make-row range data)))
                     (set-row row)
                     (funcall bind-row-closure row))))))
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
    (tagbody main
       (restart-case (check-predicate header i elt)
         (set-to-null ()
           :report "Set the row position to :null."
           (setf elt :null))
         (provide-new-value (v)
           :report "Enter the new value."
           :interactive vellum.header:read-new-value
           (setf elt v)
           (go main))))
    (setf (aref result i) elt)
    (finally (return result))))


(defmethod row-at ((header standard-header)
                   (row sequence)
                   (column integer))
  (check-type column non-negative-integer)
  (let ((length (length row)))
    (unless (< column length)
      (error 'no-column
             :bounds (iota length)
             :argument 'column
             :value column
             :format-arguments (list column)))
    (elt row column)))


(defmethod (setf row-at) (new-value
                          (header standard-header)
                          (row sequence)
                          (column integer))
  (declare (type (array t (*)) row))
  (check-type column non-negative-integer)
  (let ((length (length row)))
    (unless (< column length)
      (error 'no-column
             :bounds (iota length)
             :argument 'column
             :value column
             :format-arguments (list column)))
     (setf (elt row column) new-value)))


(defmethod (setf row-at) :around
    (new-value
     (header standard-header)
     row column)
  (tagbody main
     (block nil
       (restart-case (check-predicate header column new-value)
         (keep-old-value ()
           :report "Skip assigning the new value."
           (return nil))
         (set-to-null ()
           :report "Set the row position to :null."
           (setf new-value :null))
         (provide-new-value (v)
           :report "Enter the new value."
           :interactive read-new-value
           (setf new-value v)
           (go main)))
       (call-next-method new-value header row column))))


(defmethod (setf row-at) (new-value
                          (header standard-header)
                          (row sequence)
                          (column symbol))
  (declare (type (array t (*)) row))
  (setf (row-at header row (name-to-index header column))
        new-value))


(defmethod (setf row-at) (new-value
                          (header standard-header)
                          (row sequence)
                          (column string))
  (declare (type (array t (*)) row))
  (setf (row-at header row (name-to-index header column))
        new-value))


(defmethod row-at ((header standard-header)
                   (row sequence)
                   (column symbol))
  (~>> (name-to-index header column)
       (row-at header row)))


(defmethod row-at ((header standard-header)
                   (row sequence)
                   (column string))
  (~>> (name-to-index header column)
       (row-at header row)))


(defmethod concatenate-headers ((header standard-header)
                                &rest more-headers)
  (push header more-headers)
  (let* ((names (unique-names more-headers))
         (signatures (apply #'concatenate 'vector
                            (mapcar #'read-column-signatures more-headers))))
    (make 'standard-header :column-names names
                           :column-signatures signatures)))


(defmethod select-columns ((header standard-header)
                           columns)
  (bind ((selected (~> columns
                       (cl-ds.alg:on-each (curry #'column-signature header))
                       cl-ds.alg:to-vector))
         (names (make-hash-table :size (length selected)
                                 :test 'equal)))
    (declare (type vector selected))
    (iterate
      (for i from 0)
      (for s in-vector selected)
      (for name = (read-name s))
      (when (null name) (next-iteration))
      (when (symbolp name)
        (setf name (symbol-name name)))
      (unless (null (shiftf (gethash name names) i))
        (error 'name-duplicated
               :format-arguments (list name)
               :value name)))
    (make (class-of header)
          :column-signatures selected
          :column-names names)))


(defmethod column-specs ((header standard-header))
  (iterate
    (for signature in-vector (read-column-signatures header))
    (collect (list :name (read-name signature)
                   :predicate (read-predicate signature)
                   :type (read-type signature)))))


(defmethod bind-row-closure ((bind-row bind-row)
                             &key (header (header)))
  (funcall (optimized-closure bind-row)
           header))


(defmethod bind-row-closure ((bind-row (eql nil))
                             &key header)
  (declare (ignore header))
  (constantly nil))


(defmethod bind-row-closure (fn
                             &key header)
  (declare (ignore header))
  (ensure-function fn))


(defmethod check-predicate ((header fundamental-header)
                            column
                            value)
  (when *validate-predicates*
    (unless (funcall (column-predicate header column)
                     value)
      (let ((index (ensure-index header column)))
        (error 'predicate-failed
               :column-number index
               :format-arguments (list value column)
               :value value)))))
