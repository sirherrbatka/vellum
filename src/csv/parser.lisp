(cl:in-package #:cl-df.csv)


(defun parse-csv-line (separator escape-char skip-whitespace quote
                       line output path)
  (declare (type (simple-array fixnum (* 2)) output)
           (type (or null string) line))
  (let* ((current-state nil)
         (prev-state nil)
         (index 0)
         (input-index 0)
         (size (array-dimension output 0)))
    (declare (type fixnum index size))
    (when (null line)
      (return-from parse-csv-line nil))
    (iterate
      (for i from 0 below (array-total-size output))
      (setf (row-major-aref output i) -1))
    (labels ((handle-char (char)
               (funcall current-state char))
             (finish-column-write ()
               (setf current-state #'fresh)
               (incf index))
             (ensure-all-columns ()
               (unless (eql size index)
                 (error 'wrong-number-of-columns-in-the-csv-file
                        :path path
                        :format-control "Header defines ~a columns but file contains ~a columns."
                        :format-arguments (list size index))))
             (push-char ()
               (unless (< index size)
                 (error 'wrong-number-of-columns-in-the-csv-file
                        :path path
                        :format-control "Header defines ~a columns but file contains ~a columns."
                        :format-arguments (list size index)))
               (when (negative-fixnum-p (aref output index 0))
                 (setf (aref output index 0) input-index))
               (setf (aref output index 1) (1+ input-index)))
             (fresh (char)
               (cond ((eql char quote)
                      (setf current-state #'in-quote))
                     ((eql char separator)
                      (finish-column-write))
                     ((eql char #\newline)
                      (finish-column-write)
                      (ensure-all-columns)
                      (return-from parse-csv-line t))
                     ((serapeum:whitespacep char)
                      (unless skip-whitespace
                        (setf current-state #'in-field)
                        (in-field char)))
                     (t (setf current-state #'in-field)
                        (in-field char))))
             (in-field (char)
               (cond ((eql char separator)
                      (finish-column-write))
                     ((eql char escape-char)
                      (setf prev-state #'in-field
                            current-state #'after-escape))
                     ((eql char #\newline)
                      (finish-column-write)
                      (ensure-all-columns)
                      (return-from parse-csv-line t))
                     (t (push-char))))
             (after-escape (char)
               (declare (ignore char))
               (push-char)
               (setf current-state prev-state
                     prev-state nil))
             (after-quote (char)
               (cond ((eql char separator)
                      (finish-column-write))
                     ((eql char #\newline)
                      (finish-column-write)
                      (ensure-all-columns)
                      (return-from parse-csv-line t))
                     ((serapeum:whitespacep char)
                      (unless skip-whitespace
                        (error 'csv-format-error
                               :path path
                               :format-control "Whitespace directly after quote but whitespaces are not skipped.")))
                     (t (error 'csv-format-error
                               :path path
                               :format-control "Invalid character '~a' after closing quote."
                               :format-arguments (list char)))))
             (in-quote (char)
               (cond ((eql char escape-char)
                      (setf prev-state #'in-quote
                            current-state #'after-escape))
                     ((eql char quote)
                      (setf current-state #'after-quote))
                     (t (push-char)))))
      (setf current-state #'fresh)
      (iterate
        (for char in-vector line)
        (handle-char char)
        (incf input-index)
        (finally (return t))))))


(defun make-stream-parser (separator quote escape-char skip-whitespace)
  (curry #'parse-csv-line separator escape-char skip-whitespace quote))
