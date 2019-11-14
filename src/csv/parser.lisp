(cl:in-package #:cl-df.csv)


(declaim (inline make-csv-parsing-state-frame))
(defstruct csv-parsing-state-frame
  (line-index 0 :type cl-ds.utils:index)
  (field-index 0 :type cl-ds.utils:index)
  (in-field-index 0 :type cl-ds.utils:index)
  (line "" :type simple-string)
  (fields #() :type simple-vector))


(cl-ds.utils:define-list-of-slots csv-parsing-state-frame ()
  (line-index csv-parsing-state-frame-line-index)
  (field-index csv-parsing-state-frame-field-index)
  (in-field-index csv-parsing-state-frame-in-field-index)
  (line csv-parsing-state-frame-line)
  (fields csv-parsing-state-frame-fields))


(defun frame-rollback (old-frame new-frame)
  (declare (type csv-parsing-state-frame old-frame new-frame))
  (cl-ds.utils:with-slots-for (old-frame csv-parsing-state-frame)
    (iterate
      (for i from (1+ field-index) to (csv-parsing-state-frame-field-index new-frame))
      (setf (fill-pointer (svref fields i)) 0))
    (setf (fill-pointer (svref fields field-index)) in-field-index)))


(declaim (inline new-frame))
(defun new-frame (old-frame)
  (declare (type csv-parsing-state-frame old-frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (old-frame csv-parsing-state-frame)
    (make-csv-parsing-state-frame
     :line-index line-index
     :field-index field-index
     :in-field-index (aref fields field-index)
     :line line
     :fields fields)))


(defun frame-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (if (< line-index (length line))
        (aref line line-index)
        nil)))


(defmacro with-csv-parsing-state-frame ((state-frame char field) &body body)
  `(cl-ds.utils:with-slots-for (,state-frame csv-parsing-state-frame)
     (let ((,char (aref line line-index))
           (,field (aref fields field-index)))
       (declar)
       ,@body)))


(defun frame-put-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (vector-push-extend (frame-char frame) (aref fields field-index))
    (incf line-index)
    (incf in-field-index)))


(defun frame-skip-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (incf line-index)))


(defun frame-next-field (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (incf field-index)
    (setf in-field-index 0)))


(defun frame-validate-end (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (= (the fixnum (1+ field-index)) (length fields))))


(defmacro invoke (old-frame frame-function)
  (with-gensyms (!new-frame !result)
    (once-only (old-frame)
      `(let ((,!new-frame (new-frame old-frame)))
         (declare (dynamic-extent ,!new-frame))
         (lret ((,!result (,frame-function ,!new-frame)))
           (unless ,!result
             (frame-rollback ,old-frame ,!new-frame)))))))


(defmacro do-line ((line path fields char-name)
                   main-case
                   &body cases)
  (with-gensyms (!frame)
    (let* ((function-names (mapcar #'first (cons main-case cases)))
           (generated-function-names (mapcar (compose #'gensym #'symbol-name)
                                             (cons 'validate-end function-names)))
           (function-macrolets (mapcar (lambda (generated-name function-name)
                                         `(,function-name () (invoke ,generated-name ,!frame)))
                                       generated-function-names
                                       function-names))
           (frame-function-macrolets (mapcar (lambda (frame-function local-name)
                                               `(,local-name () (,frame-function ,!frame)))
                                             '(frame-skip-char frame-next-field frame-put-char)
                                             '(skip-char next-field put-char))))
      (once-only (line)
        `(macrolet (,@function-macrolets
                    ,@frame-function-macrolets)
           (let* ((,!frame (make-csv-parsing-state-frame
                            :line ,line
                            :fields (cl-ds.utils:transform (lambda (x)
                                                             (setf (fill-pointer x) 0)
                                                             x)
                                                           ,fields))))
             (declare (dynamic-extent ,!frame))
             (labels (,@(iterate
                          (for generated-function-name in (rest generated-function-names))
                          (for (function-name . function-body) in cases)
                          (collect `(,generated-function-name
                                     (,!frame)
                                     (declare (type csv-parsing-state-frame ,!frame))
                                     ,@function-body)))
                      (,(first generated-function-names) (,!frame)
                        (declare (type csv-parsing-state-frame ,!frame))
                        (let ((,char-name (frame-char ,!frame)))
                          ,@(rest main-case))))
               (unless (,(first generated-function-names) ,!frame)
                 (error 'csv-format-error
                        :path ,path
                        :format-control "Can't parse line ~a in the input file."
                        :format-arguments (list ,line)))
               )))))))


;; (do-line (stream new-field char)
;;   (field-char
;;    (if (null char)
;;        (validate-end)
;;        (or (and (eql char quote)
;;                 (quote-char))
;;            (and (eql char escape)
;;                 (escape-char))
;;            (and (eql separator char)
;;                 (separator-char))
;;            (ordinary-char))))
;;   (quote-char
;;    (skip-char)
;;    (ordinary-char))
;;   (ordinary-char
;;    (put-char)
;;    (field-char))
;;   (escape-char
;;    (skip-char)
;;    (ordinary-char))
;;   (separator-char
;;    (next-field)
;;    (skip-char)
;;    (field-char)))


(defun parse-csv-line (separator escape-char skip-whitespace quote
                       line output path)
  (declare (type (simple-array string (*)) output)
           (type (or null string) line))
  (let* ((current-state nil)
         (prev-state nil)
         (index 0)
         (input-index 0)
         (size (array-dimension output 0)))
    (declare (type fixnum index size))
    (when (null line)
      (return-from parse-csv-line nil))
    (clear-buffers output)
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
                        :format-arguments (list size (1+ index))))
               (vector-push-extend (aref line input-index)
                                   (aref output index)))
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


(defun make-line-parser (separator quote escape-char skip-whitespace)
  (curry #'parse-csv-line separator escape-char skip-whitespace quote))
