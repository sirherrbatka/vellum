(in-package #:cl-data-frames.header)


(defmethod alias-to-index ((header standard-header)
                           (alias symbol))
  (nth-value 0 (~>> header
                    read-column-aliases
                    (gethash alias))))


(defmethod index-to-alias ((header standard-header)
                           (index integer))
  (iterate
    (declare (type fixnum i))
    (for (alias i) in-hashtable (read-column-aliases header))
    (finding alias such-that (= index i))))
