(cl:in-package #:vellum.csv)


(defclass csv-range (cl-ds:chunking-mixin
                     cl-ds.fs:file-range-mixin
                     cl-ds:fundamental-forward-range)
  ((%separator :initarg :separator
               :reader read-separator)
   (%check-predicates :initarg :check-predicates
                      :reader read-check-predicates)
   (%quote :initarg :quote
           :reader read-quote)
   (%escape :initarg :escape
            :reader read-escape))
  (:default-initargs :initial-position 0))


(defmethod to-stream ((object t) stream)
  (prin1 object stream))


(defmethod to-stream ((object (eql :null)) stream)
  object)


(defmethod to-stream ((object local-time:timestamp) stream)
  (princ (local-time:to-rfc3339-timestring object) stream))


(defmethod from-string :around ((range csv-range) type string)
  (if (emptyp string)
      :null
      (call-next-method)))


(defmethod from-string (range (type (eql 'integer)) string)
  (nth-value 0 (parse-integer string)))


(defmethod from-string (range (type (eql 'number)) string)
  (parse-number string))


(defmethod from-string (range (type (eql t)) string)
  (take (length string) string))


(defmethod from-string (range (type (eql 'float)) string)
  (parse-float string))


(defmethod from-string (range (type (eql 'local-time:timestamp)) string)
  (local-time:parse-timestring string))


(defmethod from-string (range (type (eql 'string)) string)
  (take (length string) string))


(defmethod from-string (range (type (eql 'boolean)) string)
  (flet ((same (a b)
           (declare (type string a b))
           (and (= (length a) (length b))
                (every (lambda (a b)
                         (char-equal (char-upcase a)
                                     (char-upcase b)))
                       a b))))
    (if (or (member string '("TRUE" "T" "1") :test #'same)
            (null (or (iterate
                        (for elt in '("FALSE" "F" "NIL" "0"))
                        (finding elt such-that (same elt string)))
                      (error "Can't construct boolean from string."))))
         t nil)))


(defun make-data-buffer (size)
  (~> (make-array size :element-type 'string)
      (map-into (curry #'make-array 0 :fill-pointer 0
                                      :adjustable t
                                      :element-type 'character))))


(defmethod cl-ds:clone ((range csv-range))
  (cl-ds.fs:close-stream range)
  (make 'csv-range
        :path (cl-ds.fs:read-path range)
        :separator (read-separator range)
        :quote (read-quote range)
        :check-predicates (read-check-predicates range)
        :escape (read-escape range)
        :reached-end (cl-ds.fs:access-reached-end range)
        :initial-position (cl-ds.fs:access-current-position range)))


(defun build-strings-from-vector (range line buffers
                                  &optional (output (make-array (array-dimension buffers 0))))
  (declare (type (simple-array string (*)) buffers)
           (ignore line)
           (type string line)
           (type simple-vector output))
  (iterate
    (with check-predicates = (read-check-predicates range))
    (with header = (vellum.header:header))
    (for j from 0)
    (for buffer in-vector buffers)
    (for type = (vellum.header:column-type header j))
    (for value = (from-string range type buffer))
    (unless (or (not check-predicates)
                (funcall (vellum.header:column-predicate header j)
                         value))
      (error 'vellum.header:predicate-failed
             :column-number j
             :format-arguments (list value j)
             :value value))
    (setf (aref output j) value))
  output)


(defun read-line-buffered (stream
                           &optional (output (make-array 0
                                                         :element-type 'character
                                                         :fill-pointer 0
                                                         :adjustable t)))
  (iterate
    (for char = (read-char stream nil nil))
    (when (null char)
      (leave nil))
    (until (or (eql char #\return)
               (eql char #\newline)))
    (vector-push-extend char output)
    (finally (return output))))


(defmethod cl-ds:peek-front ((range csv-range))
  (when (cl-ds.fs:access-reached-end range)
    (return-from cl-ds:peek-front (values nil nil)))
  (let* ((stream (cl-ds.fs:ensure-stream range))
         (separator (read-separator range))
         (quote (read-quote range))
         (header (vellum:header))
         (file-position (file-position stream))
         (column-count (vellum.header:column-count header))
         (buffer (make-data-buffer column-count))
         (check-predicates (read-check-predicates range))
         (result (make-array column-count :initial-element :null))
         (path (cl-ds.fs:read-path range))
         (escape-char (read-escape range))
         (line (read-line-buffered stream)))
    (unless (file-position stream file-position)
      (error 'cl-ds:file-releated-error
             :format-control "Can't set position in the stream."
             :path path))
    (when (null line)
      (return-from cl-ds:peek-front (values nil nil)))
    (parse-csv-line separator escape-char
                    quote
                    line
                    buffer
                    path
                    (lambda (field index)
                      (bind ((trim (trim-whitespace field))
                             (type (vellum.header:column-type header index))
                             ((:values value failed)
                              (ignore-errors (from-string range type trim))))
                        (cond (failed nil)
                              ((or (not check-predicates)
                                   (funcall (vellum.header:column-predicate
                                             header index)
                                            value))
                               (setf (aref result index) value)
                               t)
                              (t nil)))))
    (values result t)))


(defmethod cl-ds:consume-front ((range csv-range))
  (when (cl-ds.fs:access-reached-end range)
    (return-from cl-ds:consume-front (values nil nil)))
  (let* ((stream (cl-ds.fs:ensure-stream range))
         (separator (read-separator range))
         (quote (read-quote range))
         (header (vellum:header))
         (column-count (vellum.header:column-count header))
         (buffer (make-data-buffer column-count))
         (check-predicates (read-check-predicates range))
         (result (make-array column-count :initial-element :null))
         (path (cl-ds.fs:read-path range))
         (escape-char (read-escape range))
         (line (read-line-buffered stream)))
    (call-next-method range)
    (when (null line)
      (return-from cl-ds:consume-front (values nil nil)))
    (parse-csv-line separator escape-char
                    quote
                    line
                    buffer
                    path
                    (lambda (field index)
                      (bind ((trim (trim-whitespace field))
                             (type (vellum.header:column-type header index))
                             ((:values value failed)
                              (ignore-errors (from-string range type trim))))
                        (cond (failed nil)
                              ((or (not check-predicates)
                                   (funcall (vellum.header:column-predicate
                                             header index)
                                            value))
                               (setf (aref result index) value)
                               t)
                              (t nil)))))
    (values result t)))


(defmethod cl-ds:traverse ((range csv-range) function)
  (declare (optimize (speed 3)))
  (unless (~> range cl-ds.fs:access-reached-end)
    (let* ((stream (cl-ds.fs:ensure-stream range))
           (separator (read-separator range))
           (quote (read-quote range))
           (column-count (~> (vellum.header:header)
                             vellum.header:column-count))
           (buffer (make-data-buffer column-count))
           (buffer2 (make-array column-count :initial-element :null))
           (header (vellum:header))
           (check-predicates (read-check-predicates range))
           (path (cl-ds.fs:read-path range))
           (line-buffer (make-array 0
                                    :element-type 'character
                                    :fill-pointer 0
                                    :adjustable t))
           (escape-char (read-escape range)))
      (vellum.header:set-row buffer2)
      (unwind-protect
           (iterate
             (for line = (read-line-buffered stream line-buffer))
             (while line)
             (parse-csv-line separator escape-char
                             quote line
                             buffer path
                             (lambda (field index)
                               (bind ((trim (trim-whitespace field))
                                      (type (vellum.header:column-type header index))
                                      ((:values value failed)
                                       (ignore-errors (from-string range type trim))))
                                 (cond (failed nil)
                                       ((or (not check-predicates)
                                            (funcall (vellum.header:column-predicate
                                                      header index)
                                                     value))
                                        (setf (aref buffer2 index) value)
                                        t)
                                       (t nil)))))
             (funcall function buffer2)
             (iterate
               (for b in-vector buffer)
               (setf (fill-pointer b) 0))
             (setf (cl-ds.fs:access-current-position range)
                   (file-position stream)
                   (fill-pointer line-buffer) 0)
             (while (peek-char t stream nil nil)))
        (setf (cl-ds.fs:access-current-position range) (file-position stream))
        (cl-ds.fs:close-stream range))))
  range)


(defmethod cl-ds:across ((range csv-range) function)
  (~> range cl-ds:clone (cl-ds:traverse function))
  range)


(defun copy-from-shared (input &key separator header quote escape check-predicates)
  (check-type separator character)
  (check-type quote character)
  (check-type escape character)
  (unless (~> (list quote separator)
              remove-duplicates
              length
              (eql 2))
    (error 'cl-ds:incompatible-arguments
           :values (list quote separator)
           :parameters '(:quote :separator)
           :format-control "Quote and separator have to be distinct from each other."))
  (with-open-file (stream input)
    (when header
      (read-line stream nil nil))
    (cl-ds.fs:with-file-ranges ((result (make 'csv-range
                                              :path input
                                              :separator separator
                                              :initial-position (file-position stream)
                                              :escape escape
                                              :quote quote
                                              :check-predicates check-predicates)))
      (cl-ds.fs:close-inner-stream result)
      (make 'vellum.header:forward-proxy-frame-range
            :original-range result))))


(defmethod vellum:copy-from ((format (eql ':csv))
                            (input string)
                            &rest options
                            &key
                              (separator #\,)
                              (header t)
                              (quote #\")
                              (escape #\\)
                              (check-predicates t))
  (declare (ignore options))
  (copy-from-shared input :separator separator
                          :header header
                          :quote quote
                          :escape escape
                          :check-predicates check-predicates))


(defmethod vellum:copy-from ((format (eql ':csv))
                            (input pathname)
                            &rest options
                            &key
                              (separator #\,)
                              (header t)
                              (quote #\")
                              (escape #\\)
                              (check-predicates t))
  (declare (ignore options))
  (copy-from-shared input :separator separator
                          :header header
                          :quote quote
                          :escape escape
                          :check-predicates check-predicates))


(defmethod vellum:copy-from ((format (eql ':csv))
                            (input cl-ds:fundamental-forward-range)
                            &rest options
                            &key
                              (separator #\,)
                              (header t)
                              (quote #\")
                              (escape #\\)
                              (check-predicates t))
  (declare (ignore options))
  (check-type separator character)
  (check-type quote character)
  (check-type escape character)
  (unless (~> (list quote separator)
              remove-duplicates
              length
              (eql 2))
    (error 'cl-ds:incompatible-arguments
           :values (list quote separator)
           :parameters '(:quote :separator)
           :format-control "Quote and separator have to be distinct from each other."))
  (bind ((result (~> input
                     (cl-ds.alg:on-each
                      (lambda (x)
                        (cl-ds.fs:with-file-ranges
                            ((inner (make 'csv-range
                                          :path x
                                          :separator separator
                                          :escape escape
                                          :quote quote
                                          :check-predicates check-predicates)))
                          (when header
                            (cl-ds:consume-front inner))
                          inner)))
                     (cl-ds.alg:without #'null)
                     (cl-ds.alg:multiplex :function #'identity))))
    (make 'vellum.header:forward-proxy-frame-range
          :original-range result)))


(defun write-row (output header row)
  (iterate
    (with column-count = (vellum.header:column-count header))
    (for column from 0 below column-count)
    (for value = (vellum.header:row-at header row column))
    (to-stream value output)
    (if (= (1+ column) column-count)
        (terpri output)
        (princ #\, output))))


(defmethod vellum:copy-to ((format (eql ':csv))
                          (output stream)
                          (input cl-ds:fundamental-forward-range)
                          &rest options
                          &key (header t))
  (declare (ignore options))
  (let* ((h (vellum.header:header))
         (column-count (vellum.header:column-count h)))
    (when header
      (iterate
        (for column from 0 below column-count)
        (for name = (vellum.header:index-to-name h column))
        (when name
          (prin1 name output))
        (unless (= (1+ column) column-count)
          (princ #\, output)))
      (terpri output))
    (cl-ds:across input
                  (lambda (&rest all)
                    (declare (ignore all))
                    (write-row output h (vellum.header:row))))
    input))


(defmethod vellum:copy-to ((format (eql ':csv))
                          (output stream)
                          (input vellum.table:standard-table)
                          &rest options
                          &key (header t))
  (declare (ignore header))
  (vellum:with-table (input)
    (apply #'vellum:copy-to format
           output
           (cl-ds:whole-range input)
           options))
  input)


(defmethod vellum:copy-to ((format (eql ':csv))
                          (output pathname)
                          input
                          &rest options
                          &key (header t)
                            (if-exists :error)
                            (if-does-not-exist :create))
  (declare (ignore header))
  (with-output-to-file (stream output
                               :if-exists if-exists
                               :if-does-not-exist if-does-not-exist)
    (apply #'vellum:copy-to format stream input options))
  input)


(defmethod vellum:copy-to ((format (eql ':csv))
                          output
                          (input cl-ds:fundamental-forward-range)
                          &rest options
                          &key (header t) (if-exists :supersede))
  (declare (ignore header))
  (with-output-to-file (stream output :if-exists if-exists)
    (apply #'vellum:copy-to format stream input options))
  input)


(defmethod vellum:copy-to ((format (eql ':csv))
                          output
                          (input vellum.table:standard-table)
                          &rest options
                          &key (header t) (if-exists :supersede))
  (declare (ignore header if-exists))
  (vellum:with-table (input)
    (apply #'vellum:copy-to format
           output
           (cl-ds:whole-range input)
           options))
  input)
