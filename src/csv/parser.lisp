(cl:in-package #:cl-df.csv)


(declaim (inline make-csv-parsing-state-frame))
(defstruct csv-parsing-state-frame
  (line-index 0 :type cl-ds.utils:index)
  (field-index 0 :type cl-ds.utils:index)
  (in-field-index 0 :type cl-ds.utils:index)
  (line "" :type simple-string)
  (fields #() :type simple-vector)
  (in-quote nil :type boolean))


(cl-ds.utils:define-list-of-slots csv-parsing-state-frame ()
  (line-index csv-parsing-state-frame-line-index)
  (field-index csv-parsing-state-frame-field-index)
  (in-field-index csv-parsing-state-frame-in-field-index)
  (line csv-parsing-state-frame-line)
  (in-quote csv-parsing-state-frame-in-quote)
  (fields csv-parsing-state-frame-fields))


(defun rollback (old-frame new-frame)
  (declare (type csv-parsing-state-frame old-frame new-frame))
  (cl-ds.utils:with-slots-for (old-frame csv-parsing-state-frame)
    (iterate
      (for i from (1+ field-index) to (csv-parsing-state-frame-field-index new-frame))
      (setf (fill-pointer (svref fields i)) 0))
    (setf (fill-pointer (svref fields field-index)) in-field-index)))


(declaim (inline new-frame))
(defun new-frame (old-frame)
  (declare (type csv-parsing-state-frame old-frame)
           (optimize (debug 3) (safety 3)))
  (cl-ds.utils:with-slots-for (old-frame csv-parsing-state-frame)
    (make-csv-parsing-state-frame
     :line-index line-index
     :field-index field-index
     :in-field-index field-index
     :line line
     :in-quote in-quote
     :fields fields)))


(defun frame-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (debug 3) (safety 3)))
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


(defun put-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (debug 3) (safety 3)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (vector-push-extend (frame-char frame) (aref fields field-index))
    (incf line-index)
    (incf in-field-index)))


(defun skip-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (debug 3) (safety 3)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (incf line-index)))


(defun next-field (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (debug 3) (safety 3)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (incf field-index)
    (setf in-field-index 0)))


(defun validate-field-number (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (debug 3) (safety 3)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (< field-index (length fields))))


(defun validate-end (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (debug 3) (safety 3)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (and (= (the fixnum (1+ field-index)) (length fields))
         (not in-quote))))


(defmacro invoke (frame-function old-frame)
  (with-gensyms (!new-frame !result)
    (once-only (old-frame)
      `(let ((,!new-frame (new-frame ,old-frame)))
         (declare (dynamic-extent ,!new-frame))
         (lret ((,!result (,frame-function ,!new-frame)))
           (unless ,!result
             (rollback ,old-frame ,!new-frame)))))))


(defmacro do-line ((line path fields char-name frame)
                   main-case
                   &body cases)
  (let* ((function-names (mapcar #'first (cons main-case cases))))
    (once-only (line)
      `(let* ((,frame (make-csv-parsing-state-frame
                       :line ,line
                       :fields (cl-ds.utils:transform (lambda (x)
                                                        (setf (fill-pointer x) 0)
                                                        x)
                                                      ,fields))))
         (declare (dynamic-extent ,frame))
         (labels (,@(iterate
                      (for (function-name . function-body) in cases)
                      (collect
                          `(,function-name
                            (,frame)
                            (declare (type csv-parsing-state-frame ,frame))
                            (cl-ds.utils:with-slots-for
                                (,frame csv-parsing-state-frame)
                              ,@function-body))))
                  (,(first function-names) (,frame)
                    (declare (type csv-parsing-state-frame ,frame))
                    (cl-ds.utils:with-slots-for
                        (,frame csv-parsing-state-frame)
                      (let ((,char-name (frame-char ,frame)))
                        (declare (type (or null character) ,char-name))
                        ,@(rest main-case)))))
           (unless (,(first function-names) ,frame)
             (error 'csv-format-error
                    :path ,path
                    :format-control "Can't parse line ~a in the input file."
                    :format-arguments (list ,line))))))))


(defun parse-csv-line (separator escape quote
                       line output path)
  (declare (type simple-vector output)
           (type character escape quote separator)
           (type simple-string line)
           (optimize (debug 3) (safety 3)))
  (do-line (line path output char frame)
      (field-char
       (if (null char)
           (validate-end frame)
           (and (validate-field-number frame)
                (or (and (eql char quote)
                         (invoke quote-char frame))
                    (and (eql char escape)
                         (if (and in-quote
                                  (not (char= escape quote)))
                             (or (invoke escape-char frame)
                                 (invoke ordinary-char frame))
                             (or (invoke ordinary-char frame)
                                 (invoke escape-char frame))))
                    (and (eql separator char)
                         (invoke separator-char frame))
                    (invoke ordinary-char frame)))))
    (quote-char
     (skip-char frame)
     (setf in-quote (not in-quote))
     (field-char frame))
    (ordinary-char
     (put-char frame)
     (field-char frame))
    (escape-char
     (skip-char frame)
     (ordinary-char frame))
    (separator-char
     (next-field frame)
     (skip-char frame)
     (field-char frame)))
  output)


(defun make-line-parser (separator quote escape-char skip-whitespace)
  (curry #'parse-csv-line separator escape-char quote))
