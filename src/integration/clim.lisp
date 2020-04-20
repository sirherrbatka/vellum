(cl:in-package #:clim-user)


(defun show-data-frame (clim-stream data-frame)
  (vellum:with-table (data-frame)
    (formatting-table (clim-stream
                       :multiple-columns t
                       :y-spacing 20
                       :x-spacing 20)
      (iter:iterate
        (iter:with header = (vellum:header))
        (iter:for i from 0 below (vellum:column-count data-frame))
        (iter:for alias = (or (ignore-errors (vellum.header:index-to-alias header i))
                              i))
        (formatting-column (clim-stream)
          (surrounding-output-with-border (clim-stream
                                           :shape :underline)
            (formatting-cell (clim-stream)
              (princ alias clim-stream)))
          (vellum:transform data-frame
                           (lambda (&rest ignore)
                             (declare (ignore ignore))
                             (formatting-cell (clim-stream)
                               (print (vellum:rr i) clim-stream)))
                           :in-place t))))))


(define-application-frame table-view ()
  ((%dataframe :initarg :dataframe :reader dataframe))
  (:pane :application
   :scroll-bars t
   :display-function (lambda (frame pane)
                       (show-data-frame pane (dataframe frame)))))


(defmethod vellum:show ((as (eql :clim))
                       (table vellum.table:fundamental-table)
                       &key)
  (run-frame-top-level
   (make-application-frame
    'table-view
    :dataframe table)))
