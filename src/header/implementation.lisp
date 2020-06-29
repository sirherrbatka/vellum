(in-package #:vellum.header)


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
