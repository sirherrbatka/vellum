(cl:in-package #:cl-df.csv)


(declaim (inline accept-eof))
(defun accept-eof (stream)
  (not (peek-char nil stream nil nil)))


(defun stream-parser-form (separator escape-char skip-whitespace quote)
  `(lambda (stream output path
            &aux
              (current-state nil)
              (prev-state nil)
              (index 0)
              (size (length output)))
     (iterate
       (for o in-vector output)
       (setf (fill-pointer o) 0))
     (block fun
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
                (push-char (char)
                  (~>> (aref output index)
                       (vector-push-extend char)))
                (fresh (char)
                  (cond ((eql char ,quote)
                         (setf current-state #'in-quote))
                        ((eql char ,separator)
                         (finish-column-write))
                        ((eql char #\newline)
                         (finish-column-write)
                         (ensure-all-columns)
                         (return-from fun t))
                        ((serapeum:whitespacep char)
                         (unless ,skip-whitespace
                           (setf current-state #'in-field)
                           (in-field char)))
                        (t (setf current-state #'in-field)
                           (in-field char))))
                (in-field (char)
                  (cond ((eql char ,separator)
                         (finish-column-write))
                        ((eql char ,escape-char)
                         (setf prev-state #'in-field
                               current-state #'after-escape))
                        ((eql char #\newline)
                         (finish-column-write)
                         (ensure-all-columns)
                         (return-from fun t))
                        ((serapeum:whitespacep char)
                         (unless ,skip-whitespace
                           (push-char char)))
                        (t (push-char char))))
                (after-escape (char)
                  (push-char char)
                  (setf current-state prev-state
                        prev-state nil))
                (after-quote (char)
                  (cond ((eql char ,separator)
                         (finish-column-write))
                        ((eql char #\newline)
                         (finish-column-write)
                         (ensure-all-columns)
                         (return-from fun t))
                        ((serapeum:whitespacep char)
                         (unless ,skip-whitespace
                           cl-ds.utils:todo))
                        (t cl-ds.utils:todo))))
                (in-quote (char)
                  (cond ((eql char ,quote)
                         (setf prev-state #'in-quote
                               current-state #'after-escape))
                        ((eql char ,quote)
                         (setf current-state #'after-quote)))))
         (setf current-state #'fresh)
         (iterate
           (for eof = (accept-eof stream))
           (when eof
             (leave nil))
           (for char = (read-char stream))
           (handle-char char))))))


(defun make-stream-parser (separator quote header)
  )
