(in-package #:cl-df)


(defun empty-column (header-class &rest row-parameters)
  (let ((header (make-header header-class row-parameters)))
    (make 'cl-df.table:standard-table
          :header header
          :columns (vector (cl-df.column:make-sparse-material-column
                            :element-type (cl-df.header:column-type
                                           header 0))))))


(defun new-columns (table &rest columns)
  (let ((new-header (~> table
                        cl-df.table:header
                        class-of
                        (cl-df:make-header columns))))
    (hstack table
            (make-table (class-of table)
                        new-header))))
