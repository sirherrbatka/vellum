(in-package #:cl-data-frames.header)


(defmethod alias-to-index ((header standard-header)
                           (alias string))
  (let* ((aliases (read-column-aliases header))
         (index (gethash alias aliases)))
    (when (null index)
      (error 'no-column
             :bounds (hash-table-keys aliases)
             :argument 'alias
             :value alias
             :format-arguments (list alias)))
    index))


(defmethod alias-to-index ((header standard-header)
                           (alias symbol))
  (alias-to-index header (symbol-name alias)))


(defmethod column-signature ((header standard-header)
                             (alias symbol))
  (column-signature header (alias-to-index header alias)))


(defmethod initialize-instance :after ((object column-signature)
                                       &key &allow-other-keys)
  (bind (((:slots %type %predicate %alias) object))
    (check-type %type (or list symbol))
    (check-type %alias (or symbol string))
    (ensure-functionf %predicate)))


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


(defmethod index-to-alias ((header standard-header)
                           (index integer))
  (check-type index non-negative-integer)
  (let* ((signature (column-signature header index))
         (alias (read-alias signature)))
    (if (null alias)
        (error 'no-column
               :bounds `(< 0 ,(~> header read-column-signatures length))
               :argument 'index
               :value index
               :format-control "No alias for column ~a."
               :format-arguments (list index))
        alias)))


(defmethod make-header (class &rest columns)
  (let* ((result (make class))
         (signature-class (read-column-signature-class result))
         (column-signatures (map 'vector
                                 (lambda (c)
                                   (apply #'make signature-class c))
                                 columns))
         (aliases (iterate
                    (with result = (make-hash-table
                                    :test 'equal
                                    :size (length column-signatures)))
                    (for column in-vector column-signatures)
                    (for i from 0)
                    (for alias = (read-alias column))
                    (when (null alias) (next-iteration))
                    (when (symbolp alias)
                      (setf alias (symbol-name alias)))
                    (unless (stringp alias)
                      (error 'invalid-alias
                             :value alias))
                    (unless (null (shiftf (gethash alias result) i))
                      (error 'alias-duplicated
                             :format-arguments (list alias)
                             :alias alias))
                    (finally (return result)))))
    (setf (slot-value result '%column-signatures) column-signatures
          (slot-value result '%column-aliases) aliases)
    result))


(defmethod column-type ((header standard-header)
                        column)
  (~>> (column-signature header column)
       read-type))


(defmethod column-predicate ((header standard-header)
                             (column integer))
  (check-type column non-negative-integer)
  (~>> (column-signature header column)
       read-predicate))


(defmethod column-count ((header standard-header))
  (~> header read-column-signatures length))


(defmethod cl-ds:consume-front ((range frame-range-mixin))
  (restart-case
      (bind (((:values data more) (call-next-method)))
        (if (no more)
            (values nil nil)
            (let ((row (make-row (header) range data)))
              (set-row row)
              (values row t))))
    (skip-row () (cl-ds:consume-front range))))


(defmethod cl-ds:peek-front ((range frame-range-mixin))
  (bind (((:values data more) (call-next-method)))
    (if (no more)
        (values nil nil)
        (let ((row (make-row (header) range data)))
          (set-row row)
          (values row t)))))


(defmethod decorate-data ((header standard-header)
                          (data cl-ds:fundamental-forward-range)
                          &key list-format)
  (make 'forward-proxy-frame-range
        :original-range (cl-ds:clone data)
        :list-format list-format
        :header header))


(defmethod make-row ((header standard-header)
                     (range frame-range-mixin)
                     (data vector))
  (iterate
    (with result = (~> header column-count
                       (make-array :initial-element :null)))
    (for i from 0)
    (for elt in-vector data)
    (setf (aref result i) (make-value header elt i))
    (finally (return result))))


(defmethod make-row ((header standard-header)
                     (range validated-frame-range-mixin)
                     (data vector))
  (iterate
    (with result = (~> header column-count
                       (make-array :initial-element :null)))
    (for i from 0)
    (for elt in-vector data)
    (setf (aref result i) elt)
    (finally (return result))))


(more-conditions:define-condition-translating-method
    make-row (header range data)
  ((error unable-to-construct-row)
   :header (header)))


(defun validate-row (row data)
  (unless (= (length (the simple-vector row)) (length data))
    (error 'invalid-input-for-row
           :format-control "Desired number of columns is not equal to number of columns in the data."
           :value data))
  row)


(defmethod make-row ((header standard-header)
                     (range frame-range-mixin)
                     (data list))
  (switch ((read-list-format range) :test eq)
    (:pair (vector (make-value header (car data) 0)
                   (make-value header (cdr data) 1)))
    (nil (iterate
           (with result = (~> header column-count
                              (make-array :initial-element nil)
                              (validate-row data)))
           (for i from 0)
           (for elt in data)
           (setf (aref result i) (make-value header elt i))
           (finally (return result))))))


(defmethod make-value ((header standard-header)
                       source
                       index)
  (unless (funcall (column-predicate header index)
                   source)
    (error 'predicate-failed
           :column-number index
           :format-arguments (list source index)
           :value source))
  source)


(defmethod row-at ((header standard-header)
                   (row vector)
                   (column integer))
  (declare (type (array t (*)) row))
  (check-type column non-negative-integer)
  (let ((length (array-dimension row 0)))
    (unless (< column length)
      (error 'no-column
             :bounds (iota length)
             :argument 'column
             :value column
             :format-arguments (list column)))
    (aref row column)))


(defmethod (setf row-at) (new-value
                          (header standard-header)
                          (row vector)
                          (column integer))
  (declare (type (array t (*)) row))
  (check-type column non-negative-integer)
  (let ((length (array-dimension row 0)))
    (unless (< column length)
      (error 'no-column
             :bounds (iota length)
             :argument 'column
             :value column
             :format-arguments (list column)))
    (unless (~> (column-predicate header column)
                (funcall new-value))
      (error 'predicate-failed
             :column-number index
             :format-arguments (list new-value column)
             :value source))
    (setf (aref row column) new-value)))


(defmethod (setf row-at) (new-value
                          (header standard-header)
                          (row vector)
                          (column symbol))
  (declare (type (array t (*)) row))
  (setf (row-at header row (alias-to-index header column))
        new-value))


(defmethod row-at ((header standard-header)
                   (row vector)
                   (column symbol))
  (~>> (alias-to-index header column)
       (row-at header row)))


(defmethod concatenate-headers ((header standard-header)
                                &rest more-headers)
  (push header more-headers)
  (let* ((aliases (unique-aliases more-headers))
         (signatures (apply #'concatenate 'vector
                            (mapcar #'read-column-signatures more-headers))))
    (make 'standard-header :column-aliases aliases
                           :column-signatures signatures)))


(defmethod select-columns ((header standard-header)
                           columns)
  (bind ((selected (~> columns
                       (cl-ds.alg:on-each (curry #'column-signature header))
                       cl-ds.alg:to-vector))
         (aliases (make-hash-table :size (length selected)
                                   :test 'equal)))
    (declare (type vector selected))
    (iterate
      (for i from 0)
      (for s in-vector selected)
      (for alias = (read-alias s))
      (when (null alias) (next-iteration))
      (unless (null (shiftf (gethash alias aliases) i))
        (error 'alias-duplicated
               :format-arguments (list alias)
               :alias alias)))
    (make (class-of header)
          :column-signatures selected
          :column-aliases aliases)))
