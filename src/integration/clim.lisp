(cl:in-package #:clim-user)


(defun show-data-frame (clim-stream data-frame)
  (cl-df:with-table (data-frame)
    (formatting-table (clim-stream)
      (formatting-row (clim-stream)
        (surrounding-output-with-border
            (clim-stream :background +grey+
                         :padding-top 0
                         :padding-bottom 0)
          (iter:iterate
            (iter:with header = (cl-df:header))
            (iter:for i from 0 below (cl-df:column-count data-frame))
            (iter:for alias = (or (ignore-errors (cl-df.header:index-to-alias header i))
                             i))
            (formatting-cell (clim-stream)
              (princ alias clim-stream)))))
      (cl-df:transform data-frame
                       (lambda (&rest ignore)
                         (declare (ignore ignore))
                         (formatting-row (clim-stream)
                           (surrounding-output-with-border
                               (clim-stream :padding-top 0
                                            :padding-bottom 0)
                             (iter:iterate
                               (iter:for i from 0 below (cl-df:column-count data-frame))
                               (formatting-cell (clim-stream)
                                 (print (cl-df:rr i) clim-stream))))))
                       :in-place t))))


(define-application-frame table-view ()
  ((%dataframe :initarg :dataframe :reader dataframe))
  (:pane :application
   :display-function (lambda (frame pane)
                       (show-data-frame pane (dataframe frame)))))


(defmethod cl-df:show ((as (eql :clim))
                       (table cl-df.table:fundamental-table)
                       &key)
  (run-frame-top-level
   (make-application-frame
    'table-view
    :dataframe table)))
