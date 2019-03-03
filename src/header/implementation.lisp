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
      (check-type type (or list symbol)))
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


(defmethod column-count ((header standard-header))
  (~> header read-column-types length))


(defmethod cl-ds:consume-front ((range frame-range-mixin))
  (bind (((:values data more) (call-next-method)))
    (if (no more)
        (values nil nil)
        (let ((row (make-row (header) range data)))
          (set-row row)
          (values row t)))))


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
                      (let ((row (make-row (header) range data)))
                        (set-row row)
                        (funcall function row)))))


(defmethod cl-ds:across ((range frame-range-mixin)
                         function)
  (ensure-functionf function)
  (call-next-method range
                    (lambda (data)
                      (let ((row (make-row (header) range data)))
                        (set-row row)
                        (funcall function row)))))


(defmethod decorate-data ((header standard-header)
                          (data cl-ds:fundamental-forward-range))
  (make 'proxy-frame-range :original-range (cl-ds:clone data)))
