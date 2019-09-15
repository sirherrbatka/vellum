(in-package #:cl-df)


(defgeneric copy-from (format input &rest options &key &allow-other-keys))

(defgeneric copy-to (format output input &rest options &key &allow-other-keys))

(defun empty-column (header-class &rest row-parameters)
  (let ((header (make-header header-class row-parameters)))
    (make 'cl-df.table:standard-table
          :header header
          :columns (vector (cl-df.column:make-sparse-material-column
                            :element-type (cl-df.header:column-type
                                           header 0))))))
