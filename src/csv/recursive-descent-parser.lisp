(cl:in-package #:cl-df.csv)


(defmacro define-parser (name &body body)
  `(defun ,name (separator escape-char skip-whitespace
                 quote line output path
                 line-index field-index result-index)
     (when (and (= field-index (length output))
                (= line-index (length line)))
       (return-from ,name t))
     (lret ((result
             (macrolet ((descent (into &key line-index field-index result-index)
                          `(,into separator escape-char skip-whitespace
                                  quote line output path
                                  ,@(list (or line-index 'line-index)
                                          (or field-index 'field-index)
                                          (or result-index 'result-index)))))
               (let ((char (aref line line-index)))
                 ,@body))))
       (unless result
         (setf (fill-pointer (aref output field-index)) result-index)))))


(defun parse-char (separator escape-char skip-whitespace quote
                   line output path line-index field-index result-index)
  (let ((char (aref line line-index)))
    ))


(defun recursive-descent-parse-csv-line (separator escape-char skip-whitespace
                                         quote line output path))
