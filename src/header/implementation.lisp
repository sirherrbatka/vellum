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


(defmethod make-header ((class (eql 'standard-header))
                        &rest columns)
  cl-ds.utils:todo)


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
