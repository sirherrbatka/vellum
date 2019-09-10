(cl:in-package #:cl-df.csv)


(defclass csv-range (cl-ds:chunking-mixin
                     cl-ds.fs:file-range-mixin
                     cl-ds:fundamental-forward-range)
  ((%separator :initarg :separator
               :reader read-separator)
   (%skip-whitespace :initarg :skip-whitespace
                     :reader read-skip-whitespace))
  (:default-initargs :initial-position 0))


(defmethod cl-ds:clone ((range csv-range))
  (cl-ds.fs:close-stream range)
  (make 'csv-range
        :path (cl-ds.fs:read-path range)
        :separator (read-separator range)
        :skip-whitespace (read-skip-whitespace range)
        :reached-end (cl-ds.fs:access-reached-end range)
        :initial-position (cl-ds.fs:access-current-position range)))


(declaim (inline accept-eof))
(defun accept-eof (stream)
  (not (peek-char nil stream nil nil)))


(defun read-csv-line (range stream)
  (if (accept-eof stream)
      :EOF
      (let ((result
              (handler-case (fare-csv:read-csv-line stream)
                (error (e)
                  (error 'cl-data-frames:file-input-row-cant-be-created
                         :cause e
                         :path (cl-ds.fs:read-path range))))))
        (if (null result)
            (if (accept-eof stream)
                :EOF
                result)
            result))))


(defun consider-eof (line)
  (if (eq line :eof)
      (values nil nil)
      (values line t)))


(defmethod cl-ds:peek-front ((range csv-range))
  (when (cl-ds.fs:access-reached-end range)
    (return-from cl-ds:peek-front (values nil nil)))
  (let* ((stream (cl-ds.fs:ensure-stream range))
         (file-position (file-position stream))
         (fare-csv:*separator* (read-separator range))
         (fare-csv:*skip-whitespace* (read-skip-whitespace range))
         (line (read-csv-line range stream)))
    (when end
      (return-from cl-ds:peek-front (values nil nil)))
    (unless (file-position stream file-position)
      (error 'cl-ds:file-releated-error
             :format-control "Can't set position in the stream."
             :path (cl-ds.fs:read-path range)))
    (consider-eof line)))


(defmethod cl-ds:consume-front ((range csv-range))
  (when (cl-ds.fs:access-reached-end range)
    (return-from cl-ds:consume-front (values nil nil)))
  (let* ((stream (cl-ds.fs:ensure-stream range))
         (fare-csv:*separator* (read-separator range))
         (fare-csv:*skip-whitespace* (read-skip-whitespace range))
         (line (read-csv-line range stream)))
    (call-next-method range)
    (consider-eof line)))


(defmethod cl-ds:traverse ((range csv-range) function)
  (unless (~> range cl-ds.fs:access-reached-end)
    (let ((stream (cl-ds.fs:ensure-stream range))
          (fare-csv:*separator* (read-separator range))
          (fare-csv:*skip-whitespace* (read-skip-whitespace range)))
      (unwind-protect
           (iterate
             (for line = (read-csv-line range stream))
             (until (eq :eof line))
             (funcall function line)
             (setf (cl-ds.fs:access-current-position range)
                   (file-position stream)))
        (setf (cl-ds.fs:access-current-position range) (file-position stream))
        (cl-ds.fs:close-stream range))))
  range)


(defmethod cl-ds:across ((range csv-range) function)
  (unless (~> range cl-ds.fs:access-reached-end)
    (unwind-protect
         (let ((position (cl-ds.fs:access-current-position range))
               (fare-csv:*separator* (read-separator range))
               (fare-csv:*skip-whitespace* (read-skip-whitespace range)))
           (with-open-file (stream (cl-ds.fs:read-path range))
             (unless (file-position stream position)
               (error 'cl-ds:file-releated-error
                      :format-control "Can't set position in the stream."
                      :path (cl-ds.fs:read-path range)))
             (iterate
               (for line = (read-line stream nil nil))
               (until (eq :eof line))
               (funcall function line))))
      (cl-ds.fs:close-stream range))) ; this is not strictly required, but it is handy.
  range)


(defmethod cl-df:copy-from ((format (eql ':csv))
                            (input pathname)
                            &rest options
                            &key
                              (separator #\,)
                              (header t)
                              (skip-whitespace t))
  (declare (ignore options))
  (let ((frame-header (cl-df.header:header)))
    (cl-ds.fs:with-file-ranges ((result (make 'csv-range
                                              :path input
                                              :separator separator
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
                              (skip-whitespace t))
  (declare (ignore options))
  (bind ((frame-header (cl-df.header:header))
         (result (~> input
                     (cl-ds.alg:on-each
                      (lambda (x)
                        (cl-ds.fs:with-file-ranges
                            ((inner (make 'csv-range
                                          :path x
                                          :separator separator
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
                          (input cl-ds:fundamental-forward-range)
                          output
                          &rest options
                          &key (header t) (if-exists :supersede))
  (declare (ignore options))
  (with-output-to-file (stream output :if-exists if-exists)
    (let* ((h (cl-df.header:header))
           (column-count (cl-df.header:column-count header)))
      (when header
        (fare-csv:write-csv-line
         (iterate
           (for column from 0 below column-count)
           (for alias = (cl-df.header:index-to-alias h column))
           (collect (if alias alias column)))
         stream))
      (cl-ds:across input
                    (lambda (row)
                      (fare-csv:write-csv-line
                       (iterate
                         (for column from 0 below column-count)
                         (collect (cl-df.header:row-at h row column)))
                       stream)))))
  input)


(defmethod cl-df:copy-to ((format (eql ':csv))
                          (input cl-df.table:standard-table)
                          output
                          &rest options
                          &key (header t) (if-exists :supersede))
  (declare (ignore header if-exists))
  (cl-df:with-table (input)
    (apply #'cl-df:copy-to format
           (cl-ds:whole-range input)
           output options))
  input)
