(in-package #:cl-df.csv)


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
  (let ((frame-header (cl-df.header:header))
        (fn (lambda (x)
              (cl-csv:read-csv-row
               x
               :separator separator
               :trim-outer-whitespace trim-outer-whitespace
               :unquoted-empty-string-is-nil unquoted-empty-string-is-nil
               :quoted-empty-string-is-nil quoted-empty-string-is-nil
               :quote quote))))
    (cl-ds.fs:with-file-ranges ((result (cl-ds.fs:line-by-line input)))
      (setf (funcall transform result))
      (when header
        (cl-ds:consume-front result))
      (cl-ds.fs:close-inner-stream result)
      (make 'cl-df.header:forward-proxy-frame-range
            :original-range (cl-ds.alg:on-each result fn)
            :header frame-header))))


(defmethod cl-df:from-file ((format (eql ':csv))
                            (input cl-ds:fundamental-forward-range)
                            &rest options
                            &key
                              (filter (constantly t))
                              (transform #'identity)
                              (separator #\,)
                              (quote #\")
                              (header t)
                              (quoted-empty-string-is-nil nil)
                              (unquoted-empty-string-is-nil t)
                              (trim-outer-whitespace t))
  (declare (ignore options))
  (let* ((frame-header (cl-df.header:header))
         (fn (lambda (x)
               (cl-csv:read-csv-row
                x
                :separator separator
                :trim-outer-whitespace trim-outer-whitespace
                :unquoted-empty-string-is-nil unquoted-empty-string-is-nil
                :quoted-empty-string-is-nil quoted-empty-string-is-nil
                :quote quote)))
         (result (~> input
                     (cl-ds.alg:on-each
                      (lambda (x)
                        (if (not (funcall filter x))
                            nil
                            (cl-ds.fs:with-file-ranges
                                ((inner (cl-ds.alg:on-each
                                         x #'cl-ds.fs:line-by-line)))
                              (setf inner (funcall transform x))
                              (when header
                                (cl-ds:consume-front inner))
                              (cl-ds.alg:on-each inner fn)))))
                     (cl-ds.alg:without #'null)
                     cl-ds.alg:chain-traversable)))
    (make 'cl-df.header:forward-proxy-frame-range
          :original-range result
          :header frame-header)))
