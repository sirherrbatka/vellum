(cl:in-package #:cl-df.csv)


(declaim (notinline make-csv-parsing-state-frame))
(defstruct csv-parsing-state-frame
  (callback #'identity :type function)
  previous-frame
  (line-index 0 :type cl-ds.utils:index)
  (field-index 0 :type cl-ds.utils:index)
  (in-field-index 0 :type cl-ds.utils:index)
  (line "" :type string)
  (fields #() :type simple-vector)
  (in-quote nil :type boolean)
  (next-frames '() :type list))


(cl-ds.utils:define-list-of-slots csv-parsing-state-frame ()
  (previous-frame csv-parsing-state-frame-previous-frame)
  (line-index csv-parsing-state-frame-line-index)
  (field-index csv-parsing-state-frame-field-index)
  (in-field-index csv-parsing-state-frame-in-field-index)
  (line csv-parsing-state-frame-line)
  (fields csv-parsing-state-frame-fields)
  (in-quote csv-parsing-state-frame-in-quote)
  (next-frames csv-parsing-state-frame-next-frames)
  (callback csv-parsing-state-frame-callback))


(defun rollback (frame)
  (let ((new-frame frame)
        (old-frame (csv-parsing-state-frame-previous-frame frame)))
    (declare (type csv-parsing-state-frame new-frame)
             (type (or null csv-parsing-state-frame) old-frame))
    (when (null old-frame)
      (return-from rollback nil))
    (cl-ds.utils:with-slots-for (old-frame csv-parsing-state-frame)
      (iterate
        (for i from (1+ field-index)
             to (min (csv-parsing-state-frame-field-index new-frame)
                     (1- (length fields))))
        (setf (fill-pointer (svref fields i)) 0))
      (when (< field-index (length fields))
        (setf (fill-pointer (svref fields field-index)) in-field-index)))
    old-frame))


(declaim (notinline new-frame))
(defun new-frame (old-frame function)
  (declare (type csv-parsing-state-frame old-frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (old-frame csv-parsing-state-frame)
    (make-csv-parsing-state-frame
     :callback function
     :previous-frame old-frame
     :line-index line-index
     :field-index field-index
     :in-field-index in-field-index
     :line line
     :fields fields
     :in-quote in-quote)))


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


(defun put-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (vector-push-extend (frame-char frame) (aref fields field-index))
    (incf line-index)
    (incf in-field-index)))


(defun skip-char (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (incf line-index)))


(defun next-field (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (incf field-index)
    (setf in-field-index 0)))


(defun validate-field (frame field-predicate)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (funcall field-predicate
             (aref fields field-index)
             field-index)))


(defun validate-field-number (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (< field-index (length fields))))


(defun validate-end (frame)
  (declare (type csv-parsing-state-frame frame)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
    (and (= (the fixnum (1+ field-index)) (length fields))
         (not in-quote))))


(defmacro invoke (frame-function old-frame)
  (once-only (old-frame)
    `(progn (push (new-frame ,old-frame (function ,frame-function))
                  (csv-parsing-state-frame-next-frames ,old-frame))
            ,old-frame)))


(defun evalute-frame (frame)
  (if (null frame)
      nil
      (cl-ds.utils:with-slots-for (frame csv-parsing-state-frame)
        (funcall callback frame)
        frame)))


(defun unfold-frame (old-frame)
  (cl-ds.utils:with-slots-for (old-frame csv-parsing-state-frame)
    (if (endp next-frames)
        (progn (rollback old-frame)
               previous-frame)
        (bind (((first . rest) next-frames))
          (setf next-frames rest)
          (evalute-frame first)))))


(defun line-parsed-p (frame)
  (if (null (frame-char frame))
      (validate-end frame)
      nil))


(defmacro do-line ((line path fields char-name frame)
                   main-case
                   &body cases)
  (let* ((function-names (mapcar #'first (cons main-case cases))))
    (with-gensyms (!frame)
      (once-only (line)
        `(let* ((,frame (make-csv-parsing-state-frame
                         :line ,line
                         :fields (cl-ds.utils:transform
                                  (lambda (x) (setf (fill-pointer x) 0) x)
                                  ,fields))))
           (declare (dynamic-extent ,frame))
           (labels (,@(iterate
                        (for (function-name . function-body) in cases)
                        (collect
                            `(,function-name
                              (,frame)
                              (declare (type csv-parsing-state-frame ,frame))
                              (cl-ds.utils:with-slots-for (,frame csv-parsing-state-frame)
                                ,@function-body))))
                    (,(first function-names) (,frame)
                      (declare (type csv-parsing-state-frame ,frame))
                      (cl-ds.utils:with-slots-for
                          (,frame csv-parsing-state-frame)
                        (let ((,char-name (frame-char ,frame)))
                          (declare (type (or null character) ,char-name))
                          ,@(rest main-case)))))
             (setf (csv-parsing-state-frame-callback ,frame)
                   (function ,(first function-names)))
             (evalute-frame ,frame)
             (iterate
               (for ,!frame = (unfold-frame ,frame))
               (while ,!frame)
               (cond ((not (validate-field-number ,!frame))
                      (setf ,frame (rollback ,!frame))
                      (next-iteration))
                     ((line-parsed-p ,!frame)
                      (leave t))
                     (t (setf ,frame ,!frame)))
               (finally
                (error 'csv-format-error
                       :path ,path
                       :format-control "Can't parse line ~a in the input file."
                       :format-arguments (list ,line))))))))))


(def constantly-t (constantly t))


(defun parse-csv-line (separator escape quote
                       line output path
                       &optional (field-predicate constantly-t))
  (declare (type simple-vector output)
           (type character escape quote separator)
           (type string line)
           (optimize (speed 3) (safety 0) (debug 0)))
  (do-line (line path output char frame)
      (field-char
       (if (null char)
           (validate-field frame field-predicate)
           (progn
             (invoke ordinary-char frame)
             (when (eql separator char)
               (if in-quote
                   (invoke ordinary-char frame)
                   (invoke separator-char frame)))
             (when (eql char quote)
               (invoke quote-char frame))
             (when (eql char escape)
               (invoke escape-char frame))))
       frame)
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
     (when (validate-field frame field-predicate)
       (skip-char frame)
       (next-field frame)
       (field-char frame))))
  output)
