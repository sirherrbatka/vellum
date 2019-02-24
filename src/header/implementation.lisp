(in-package #:cl-data-frames.header)


(defmethod alias-to-index ((header standard-header)
                           (alias symbol))
  ;; should raise exception if column was not found...
  (nth-value 0 (~>> header
                    read-column-aliases
                    (gethash alias))))


(defmethod index-to-alias ((header standard-header)
                           (index integer))
  ;; should raise exception if column was not found...
  (iterate
    (declare (type fixnum i))
    (for (alias i) in-hashtable (read-column-aliases header))
    (finding alias such-that (= index i))))


(defmethod make-header ((class (eql 'standard-header))
                        &rest columns)
  cl-ds.utils:todo)


(defmethod column-type ((header standard-header)
                        (column symbol))
  (let ((index (alias-to-index header column)))
    (column-type header index)))


(defmethod column-type ((header standard-header)
                        (column integer))
  (~> header read-column-types (aref column)))
