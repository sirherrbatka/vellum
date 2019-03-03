(in-package #:cl-data-frames.header)


(defmethod alias-to-index ((header standard-header)
                           (alias symbol))
  (let* ((aliases (read-column-aliases header))
         (index (gethash alias aliases)))
    (when (null index)
      (error 'no-column
             :bounds (hash-table-keys aliases)
             :value alias
             :text "No column with such alias."))
    index))


(defmethod index-to-alias ((header standard-header)
                           (index integer))
  (check-type index non-negative-integer)
  (or (iterate
        (declare (type fixnum i))
        (for (alias i) in-hashtable (read-column-aliases header))
        (finding alias such-that (= index i)))
      (error 'no-column
             :bounds (~> header
                         read-column-aliases
                         hash-table-values)
             :value index
             :text "No alias with such index.")))


(defmethod validate-column-specification ((class (eql 'standard-header))
                                          column-specification)
  ;; column-specification may or may not contain the type and alias
  ;; we will ignore the unknown/unsupported types so this specific method will
  ;; be usable not only alone, but also as a part of the extended validation
  ;; in subclasses of the standard-header.
  (let ((type (getf column-specification :type))
        (alias (getf column-specification :alias))
        (predicate (getf column-specification :predicate)))
    (unless (null type)
      (check-type type symbol))
    (unless (null alias)
      (check-type alias symbol))
    (unless (null predicate)
      (ensure-function predicate))
    column-specification))


(defmethod make-header :before ((class symbol) &rest columns)
  (map nil
       (curry #'validate-column-specification class)
       columns))


(defmethod make-header ((class (eql 'standard-header))
                        &rest columns)
  (make 'standard-header
        :column-aliases (iterate
                          (with result = (make-hash-table
                                          :size (length columns)))
                          (for column in columns)
                          (for i from 0)
                          (for alias = (getf column :alias))
                          (unless (null alias)
                            (setf (gethash alias result) i))
                          (finally (return result)))
        :predicates (map 'vector
                         (cl-ds.utils:or* (rcurry #'getf :predicate)
                                          (constantly (constantly t)))
                         columns)
        :column-types (map 'vector
                           (cl-ds.utils:or* (rcurry #'getf :type)
                                            (constantly t))
                           columns)))


(defmethod column-type ((header standard-header)
                        (column symbol))
  (~>> (alias-to-index header column)
       (column-type header)))


(defmethod column-type ((header standard-header)
                        (column integer))
  (check-type column non-negative-integer)
  (let* ((types (read-column-types header))
         (length (length types)))
    (unless (< column length)
      (error 'no-column
             :bounds (iota length)
             :value column
             :text "No column with such index."))
    (aref types column)))


(defmethod column-predicate ((header standard-header)
                             (column integer))
  (check-type column non-negative-integer)
  (let* ((predicates (read-predicates header))
         (length (length predicates)))
    (unless (< column length)
      (error 'no-column
             :bounds (iota length)
             :value column
             :text "No column with such index."))
    (aref predicates column)))


(defmethod column-predicate ((header standard-header)
                             (column symbol))
  (~>> (alias-to-index header column)
       (column-predicate header)))


(defmethod column-count ((header standard-header))
  (~> header read-column-types length))


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


(defmethod cl-ds:traverse ((range frame-range-mixin)
                           function)
  (ensure-functionf function)
  (call-next-method range
                    (lambda (data)
                      (restart-case
                          (let ((row (make-row (header) range data)))
                            (set-row row)
                            (funcall function row))
                        (skip-row () nil)))))


(defmethod cl-ds:across ((range frame-range-mixin)
                         function)
  (ensure-functionf function)
  (call-next-method range
                    (lambda (data)
                      (restart-case
                          (let ((row (make-row (header) range data)))
                            (set-row row)
                            (funcall function row))
                        (skip-row () nil)))))


(defmethod decorate-data ((header standard-header)
                          (data cl-ds:fundamental-forward-range))
  (make 'forward-proxy-frame-range :original-range (cl-ds:clone data)))


(defmethod make-row ((header standard-header)
                     (range frame-range-mixin)
                     (data vector))
  (iterate
    (with result = (make-array (length data)))
    (for i from 0)
    (for elt in-vector data)
    (setf (aref result i) (make-value header elt i))
    (finally (return result))))


(defmethod make-row ((header standard-header)
                     (range frame-range-mixin)
                     (data list))
  (iterate
    (with result = (make-array (length data)))
    (for i from 0)
    (for elt in data)
    (setf (aref result i) (make-value header elt i))
    (finally (return result))))


(defmethod make-value ((header standard-header)
                       source
                       index)
  (lret ((result (convert source (column-type header index))))
    (unless (funcall (column-predicate header index)
                     result)
      (error 'predicate-failed))))


(defmethod convert ((value string)
                    (type (eql 'integer)))
  (handler-case (parse-integer value)
    (error (e)
      (declare (ignore e))
      (error 'conversion-failed))))


(defmethod convert ((value string)
                    (type (eql 'float)))
  (handler-case (parse-float value)
    (error (e)
      (declare (ignore e))
      (error 'conversion-failed))))


(defmethod convert ((value (eql nil))
                    type)
  :null)


(defmethod convert ((value string)
                    (type (eql 'number)))
  (handler-case (parse-number value)
    (error (e)
      (declare (ignore e))
      (error 'conversion-failed))))


(defmethod convert ((value string)
                    (type (eql 'string)))
  value)


(defmethod convert (value
                    (type (eql 't)))
  value)


(defmethod convert ((value string)
                    (type (eql 'boolean)))
  (flet ((same (a b)
           (declare (type string a b))
           (and (= (length a) (length b))
                (every (lambda (a b)
                         (char-equal (char-upcase a)
                                     (char-upcase b)))
                       a b))))
    (or (member value '("TRUE" "T" "1") :test #'same)
        (null (iterate
                (for elt in '("FALSE" "F" "NIL" "0"))
                (finding elt such-that (same elt value))
                (finally (error 'conversion-failed)))))))


(defmethod row-at ((header standard-header)
                   (row vector)
                   (column integer))
  (declare (type (array t (*)) row))
  (check-type column non-negative-integer)
  (let ((length (array-dimension row 0)))
    (unless (< column length)
      (error 'no-column
             :bounds (iota length)
             :value column
             :text "No column with such index."))
    (aref row column)))


(defmethod row-at ((header standard-header)
                   (row vector)
                   (column symbol))
  (~>> (alias-to-index header column)
       (row-at header row)))
