(in-package #:cl-df.csv)


(defmethod cl-df:from-stream ((format (eql ':csv))
                              input
                              &rest options
                              &key
                                (separator #\,)
                                (quote #\")
                                (header t)
                                (quoted-empty-string-is-nil nil)
                                (unquoted-empty-string-is-nil t)
                                (trim-outer-whitespace t))
  (declare (ignore options))
  (let* ((fn (lambda (x)
               (cl-csv:read-csv-row
                x
                :separator separator
                :trim-outer-whitespace trim-outer-whitespace
                :unquoted-empty-string-is-nil unquoted-empty-string-is-nil
                :quoted-empty-string-is-nil quoted-empty-string-is-nil
                :quote quote)))
         (result (~> input cl-ds.fs:line-by-line
                     (cl-ds.alg:on-each fn))))
    (when header
      (cl-ds:consume-front result))
    (make 'cl-df.header:forward-proxy-frame-range
          :original-range result)))
