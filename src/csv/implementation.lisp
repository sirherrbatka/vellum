(cl:in-package #:cl-df.csv)


(defclass csv-range (cl-ds:chunking-mixin
                     cl-ds.fs:file-range-mixin
                     cl-ds:fundamental-forward-range)
  ((%separator :initarg :separator
               :reader read-separator)
   (%quote :initarg :quote
           :reader read-quote)
   (%escape :initarg :escape
            :reader read-escape)
   (%skip-whitespace :initarg :skip-whitespace
                     :reader read-skip-whitespace))
  (:default-initargs :initial-position 0))


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
        :escape (read-escape range)
        :skip-whitespace (read-skip-whitespace range)
        :reached-end (cl-ds.fs:access-reached-end range)
        :initial-position (cl-ds.fs:access-current-position range)))


(defun build-strings-from-vector (line buffers
                                  &optional (output (make-array (array-dimension buffers 0))))
  (declare (type (simple-array string (*)) buffers)
           (ignore line)
           (type string line)
           (type simple-vector output))
  (iterate
    (for j from 0)
    (for buffer in-vector buffers)
    (setf (aref output j) (iterate
                            (with result = (make-string (length buffer)))
                            (for c in-vector buffer)
                            (for i from 0)
                            (setf (aref result i) c)
                            (finally (return result)))))
  output)


(defmethod cl-ds:peek-front ((range csv-range))
  (when (cl-ds.fs:access-reached-end range)
    (return-from cl-ds:peek-front (values nil nil)))
  (let* ((stream (cl-ds.fs:ensure-stream range))
         (file-position (file-position stream))
         (separator (read-separator range))
         (skip-whitespace (read-skip-whitespace range))
         (quote (read-quote range))
         (buffer (~> (cl-df.header:header)
                     cl-df.header:column-count
                     make-data-buffer))
         (path (cl-ds.fs:read-path range))
         (escape-char (read-escape range))
         (line (read-line stream nil nil))
         (status (parse-csv-line separator escape-char
                               skip-whitespace
                               quote
                               line
                               buffer
                               path)))
    (unless (file-position stream file-position)
      (error 'cl-ds:file-releated-error
             :format-control "Can't set position in the stream."
             :path path))
    (if (null status)
        (values nil nil)
        (values (build-strings-from-vector line buffer)
                t))))


(defmethod cl-ds:consume-front ((range csv-range))
  (when (cl-ds.fs:access-reached-end range)
    (return-from cl-ds:consume-front (values nil nil)))
  (let* ((stream (cl-ds.fs:ensure-stream range))
         (separator (read-separator range))
         (skip-whitespace (read-skip-whitespace range))
         (quote (read-quote range))
         (buffer (~> (cl-df.header:header)
                     cl-df.header:column-count
                     make-data-buffer))
         (path (cl-ds.fs:read-path range))
         (escape-char (read-escape range))
         (line (read-line stream nil nil))
         (status (parse-csv-line separator escape-char
                               skip-whitespace
                               quote
                               line
                               buffer
                               path)))
    (call-next-method range)
    (if (null status)
        (values nil nil)
        (values (build-strings-from-vector line buffer)
                t))))


(defmethod cl-ds:traverse ((range csv-range) function)
  (unless (~> range cl-ds.fs:access-reached-end)
    (let* ((stream (cl-ds.fs:ensure-stream range))
           (separator (read-separator range))
           (skip-whitespace (read-skip-whitespace range))
           (quote (read-quote range))
           (column-count (~> (cl-df.header:header)
                             cl-df.header:column-count))
           (buffer (make-data-buffer column-count))
           (buffer2 (make-array column-count))
           (path (cl-ds.fs:read-path range))
           (escape-char (read-escape range)))
      (unwind-protect
           (iterate
             (for line = (read-line stream nil nil))
             (for status = (parse-csv-line separator escape-char
                                           skip-whitespace
                                           quote
                                           line
                                           buffer
                                           path))
             (while status)
             (funcall function
                      (build-strings-from-vector line buffer buffer2))
             (setf (cl-ds.fs:access-current-position range)
                   (file-position stream)))
        (setf (cl-ds.fs:access-current-position range) (file-position stream))
        (cl-ds.fs:close-stream range))))
  range)


(defmethod cl-ds:across ((range csv-range) function)
  (unless (~> range cl-ds.fs:access-reached-end)
    (unwind-protect
         (let* ((separator (read-separator range))
                (skip-whitespace (read-skip-whitespace range))
                (quote (read-quote range))
                (column-count (~> (cl-df.header:header)
                                  cl-df.header:column-count))
                (buffer (make-data-buffer column-count))
                (buffer2 (make-array column-count))
                (position (cl-ds.fs:access-current-position range))
                (path (cl-ds.fs:read-path range))
                (escape-char (read-escape range)))
           (with-open-file (stream (cl-ds.fs:read-path range))
             (unless (file-position stream position)
               (error 'cl-ds:file-releated-error
                      :format-control "Can't set position in the stream."
                      :path (cl-ds.fs:read-path range)))
             (iterate
               (for line = (read-line stream nil nil))
               (for status = (parse-csv-line separator escape-char
                                             skip-whitespace
                                             quote
                                             line
                                             buffer
                                             path))
               (while status)
               (funcall function
                        (build-strings-from-vector line buffer buffer2))
               (setf (cl-ds.fs:access-current-position range)
                     (file-position stream)))))
      (cl-ds.fs:close-stream range))) ; this is not strictly required, but it is handy.
  range)


(defmethod cl-df:copy-from ((format (eql ':csv))
                            (input pathname)
                            &rest options
                            &key
                              (separator #\,)
                              (header t)
                              (quote #\")
                              (escape #\\)
                              (skip-whitespace t))
  (declare (ignore options))
  (check-type separator character)
  (check-type quote character)
  (check-type escape character)
  (unless (~> (list quote escape separator)
              remove-duplicates
              length
              (eql 3))
    (error 'cl-ds:incompatible-arguments
           :values (list quote escape separator)
           :parameters '(:quote :escape :separator)
           :format-control "Quote, escape and separator have to be distinct from each other."))
  (let ((frame-header (cl-df.header:header)))
    (cl-ds.fs:with-file-ranges ((result (make 'csv-range
                                              :path input
                                              :separator separator
                                              :escape escape
                                              :quote quote
                                              :skip-whitespace skip-whitespace)))
      (when header
        (cl-ds:consume-front result))
      (cl-ds.fs:close-inner-stream result)
      (make 'cl-df.header:forward-proxy-frame-range
            :original-range (cl-ds:clone result)
            :header frame-header))))


(defmethod cl-df:copy-from ((format (eql ':csv))
                            (input cl-ds:fundamental-forward-range)
                            &rest options
                            &key
                              (separator #\,)
                              (header t)
                              (quote #\")
                              (escape #\\)
                              (skip-whitespace t))
  (declare (ignore options))
  (check-type separator character)
  (check-type quote character)
  (check-type escape character)
  (unless (~> (list quote escape separator)
              remove-duplicates
              length
              (eql 3))
    (error 'cl-ds:incompatible-arguments
           :values (list quote escape separator)
           :parameters '(:quote :escape :separator)
           :format-control "Quote, escape and separator have to be distinct from each other."))
  (bind ((frame-header (cl-df.header:header))
         (result (~> input
                     (cl-ds.alg:on-each
                      (lambda (x)
                        (cl-ds.fs:with-file-ranges
                            ((inner (make 'csv-range
                                          :path x
                                          :separator separator
                                          :escape escape
                                          :quote quote
                                          :skip-whitespace skip-whitespace)))
                          (when header
                            (cl-ds:consume-front inner))
                          inner)))
                     (cl-ds.alg:without #'null)
                     cl-ds.alg:chain-traversable)))
    (make 'cl-df.header:forward-proxy-frame-range
          :original-range (cl-ds:clone result)
          :header frame-header)))


(defmethod cl-df:copy-to ((format (eql ':csv))
                          (output stream)
                          (input cl-ds:fundamental-forward-range)
                          &rest options
                          &key (header t))
  (declare (ignore options))
  (let* ((h (cl-df.header:header))
         (column-count (cl-df.header:column-count header)))
    (when header
      (iterate
        (for column from 0 below column-count)
        (for alias = (cl-df.header:index-to-alias h column))
        (when alias
          (princ alias output))
        (unless (= (1+ column) column-count)
          (princ #\, output)))
      (terpri output))
    (cl-ds:across input
                  (lambda (row)
                    (iterate
                      (for column from 0 below column-count)
                      (prin1 (cl-df.header:row-at h row column) output)
                      (if (= (1+ column) column-count)
                          (terpri output)
                          (princ #\, output)))))
    input))


(defmethod cl-df:copy-to ((format (eql ':csv))
                          output
                          (input cl-ds:fundamental-forward-range)
                          &rest options
                          &key (header t) (if-exists :supersede))
  (declare (ignore header))
  (with-output-to-file (stream output :if-exists if-exists)
    (apply #'cl-df:copy-to format stream input options))
  input)


(defmethod cl-df:copy-to ((format (eql ':csv))
                          output
                          (input cl-df.table:standard-table)
                          &rest options
                          &key (header t) (if-exists :supersede))
  (declare (ignore header if-exists))
  (cl-df:with-table (input)
    (apply #'cl-df:copy-to format
           (cl-ds:whole-range input)
           output options))
  input)
