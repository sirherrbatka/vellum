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
        (alias (getf column-specification :alias)))
    (unless (null type)
      (check-type type (or list symbol)))
    (unless (null alias)
      (check-type alias symbol))
    column-specification))


(defmethod make-header :before ((class symbol) &rest columns)
  (map nil (curry #'validate-column-specification class) columns))


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
        :type (map 'vector
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
