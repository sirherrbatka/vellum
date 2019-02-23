(in-package #:cl-data-frames.header)


(defmethod alias-to-index ((header standard-header)
                           (alias symbol))
  (nth-value 0 (~>> header
                    read-column-aliases
                    (gethash alias))))
