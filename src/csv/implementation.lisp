(in-package #:cl-df.csv)


(defun parsing-function (pathname separator trim-outer-whitespace unquoted-empty-string-is-nil
                         quoted-empty-string-is-nil quote)
  (lambda (x)
    (handler-case
        (cl-csv:read-csv-row
         x
         :separator separator
         :trim-outer-whitespace trim-outer-whitespace
         :unquoted-empty-string-is-nil unquoted-empty-string-is-nil
         :quoted-empty-string-is-nil quoted-empty-string-is-nil
         :quote quote)
      (cl-csv:csv-parse-error (e)
        (error 'cl-data-frames:row-cant-be-created
               :cause e
               :pathname pathname)))))


(defmethod cl-df:from-file ((format (eql ':csv))
                            (input pathname)
                            &rest options
                            &key
                              (separator #\,)
                              (transform #'identity)
                              (quote #\")
                              (header t)
                              (quoted-empty-string-is-nil nil)
                              (unquoted-empty-string-is-nil t)
                              (trim-outer-whitespace t))
  (declare (ignore options))
  (let ((frame-header (cl-df.header:header)))
    (cl-ds.fs:with-file-ranges ((result (cl-ds.fs:line-by-line input)))
      (setf result (funcall transform result))
      (when header
        (cl-ds:consume-front result))
      (cl-ds.fs:close-inner-stream result)
      (make 'cl-df.header:forward-proxy-frame-range
            :original-range (cl-ds.alg:on-each
                             result
                             (parsing-function input
                                               separator
                                               trim-outer-whitespace
                                               unquoted-empty-string-is-nil
                                               quoted-empty-string-is-nil
                                               quote))
            :header frame-header))))


(defmethod cl-df:from-file ((format (eql ':csv))
                            (input cl-ds:fundamental-forward-range)
                            &rest options
                            &key
                              (transform #'identity)
                              (separator #\,)
                              (quote #\")
                              (header t)
                              (quoted-empty-string-is-nil nil)
                              (unquoted-empty-string-is-nil t)
                              (trim-outer-whitespace t))
  (declare (ignore options))
  (bind ((frame-header (cl-df.header:header))
         (result (~> input
                     (cl-ds.alg:on-each
                      (lambda (x)
                        (cl-ds.fs:with-file-ranges
                            ((inner (cl-ds.fs:line-by-line x)))
                          (setf inner (funcall transform inner))
                          (when header
                            (cl-ds:consume-front inner))
                          (cl-ds.alg:on-each inner
                                             (parsing-function
                                              x
                                              separator
                                              trim-outer-whitespace
                                              unquoted-empty-string-is-nil
                                              quoted-empty-string-is-nil
                                              quote)))))
                     (cl-ds.alg:without #'null)
                     cl-ds.alg:chain-traversable)))
    (make 'cl-df.header:forward-proxy-frame-range
          :original-range result
          :header frame-header)))
