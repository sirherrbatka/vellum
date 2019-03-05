(in-package #:cl-df.table)


(defmethod at ((frame standard-table)
               (column symbol)
               (row integer))
  (~> frame header
      (cl-df.header:alias-to-index column)
      (at frame _ row)))


(defmethod at ((frame standard-table)
               (column integer)
               (row integer))
  (check-type column non-negative-integer)
  (check-type row non-negative-integer))


(defmethod (setf at) (new-value
                      (frame standard-table)
                      (column symbol)
                      (row integer))
  (setf (at frame (cl-df.header:alias-to-index (header frame)
                                               column)
            row)
        new-value))
