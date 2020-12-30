(cl:in-package #:vellum)


(defgeneric copy-from (format input &rest options &key &allow-other-keys))

(defgeneric copy-to (format output input &rest options &key &allow-other-keys))

(defgeneric show (as table &key &allow-other-keys))

(defmethod show ((as (eql :text))
                 (table vellum.table:fundamental-table)
                 &key
                   (output *standard-output*)
                   (start 0)
                   (end 10))
  (check-type table vellum.table:fundamental-table)
  (check-type output stream)
  (check-type start non-negative-integer)
  (check-type end non-negative-integer)
  (bind ((column-count (vellum:column-count table))
         (end (min end (row-count table)))
         (number-of-rows (max 0 (- end start)))
         (strings (make-array `(,(1+ number-of-rows) ,column-count)))
         (header (vellum.table:header table))
         (desired-sizes (make-array column-count
                                    :element-type 'fixnum
                                    :initial-element 0))
         ((:flet print-with-padding (row column))
          (let* ((string (aref strings row column))
                 (length (length string))
                 (desired-length (+ 2 (aref desired-sizes column))))
            (format output "~A" string)
            (unless (= (1+ column) column-count)
              (dotimes (i (- desired-length length))
                (format output "~a" #\space))))))
    (format output
            "~a columns × ~a rows. Printed rows from ~a below ~a:~%"
            column-count
            (row-count table)
            (min start end)
            end)
    (iterate
      (for j from 0 below column-count)
      (for string = (or (ignore-errors
                         (~> header
                             (vellum.header:index-to-name j)
                             symbol-name))
                        (format nil "~a" j)))
      (setf (aref strings 0 j) string)
      (setf (aref desired-sizes j) (length string)))
    (iterate
      (for i from start below end)
      (for row from 1)
      (iterate
        (for j from 0 below column-count)
        (for string = (princ-to-string (at table i j)))
        (setf (aref strings row j) string)
        (maxf (aref desired-sizes j) (length string))))
    (iterate
      (for j from 0 below column-count)
      (print-with-padding 0 j))
    (terpri output)
    (dotimes (i (+ (reduce #'+ desired-sizes)
                   (* (1- column-count)
                      2)))
      (princ #\= output))
    (terpri)
    (iterate
      (for i from 1 to number-of-rows)
      (iterate
        (for j from 0 below column-count)
        (print-with-padding i j))
      (terpri output))))


(defgeneric join (algorithm method frame-specs &key header class header-class columns
                  &allow-other-keys))


(defmethod join :before (algorithm method (frame-specs list) &key &allow-other-keys)
  (when (emptyp frame-specs)
    (error 'cl-ds:invalid-argument-value
           :value frame-specs
           :argument frame-specs
           :format-control "Empty frame-specs list, nothing to join.")))
