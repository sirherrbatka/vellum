(cl:in-package #:cl-df.csv)


(defgeneric to-stream (object stream))

(defgeneric from-string (range type string))

(more-conditions:define-condition-translating-method from-string
    (range type string)
  ((error csv-format-error)
   :format-control "Can't parse ~a to construct ~a."
   :format-arguments (list string type)))
