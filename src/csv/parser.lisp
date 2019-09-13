(cl:in-package #:cl-df.csv)


(defun parse-csv-line (separator escape-char skip-whitespace quote
                       stream output path)
  (let* ((current-state nil)
        (prev-state nil)
        (index 0)
         (size (length output)))
    (declare (type fixnum index size)
             (type (simple-array string (*)) output)
             (type stream stream))
    (assert (input-stream-p stream))
    (iterate
      (for o in-vector output)
      (setf (fill-pointer o) 0))
    (labels ((handle-char (char)
               (funcall current-state char))
             (finish-column-write ()
               (setf current-state #'fresh)
               (print (aref output index))
               (incf index))
             (ensure-all-columns ()
               (unless (eql size index)
                 (error 'wrong-number-of-columns-in-the-csv-file
                        :path path
                        :format-control "Header defines ~a columns but file contains ~a columns."
                        :format-arguments (list size index))))
             (push-char (char)
               (unless (< index size)
                 (error 'wrong-number-of-columns-in-the-csv-file
                        :path path
                        :format-control "Header defines ~a columns but file contains ~a columns."
                        :format-arguments (list size index)))
               (~>> (aref output index)
                    (vector-push-extend char)))
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
                     (t (push-char char))))
             (after-escape (char)
               (push-char char)
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
                     (t (push-char char)))))
      (setf current-state #'fresh)
      (iterate
        (for char = (read-char stream nil nil))
        (when (null char)
          (leave (not (zerop index))))
        (handle-char char)
        (finally (return t))))))


(defun make-stream-parser (separator quote escape-char skip-whitespace)
  (curry #'parse-csv-line separator escape-char skip-whitespace quote))
