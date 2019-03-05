(in-package #:cl-df.table)


(defmethod at ((frame standard-table)
               (column symbol)
               (row integer))
  (at frame
      (~> column header (cl-df.header:alias-to-index column))
      row))


(defmethod at ((frame standard-table)
               (column integer)
               (row integer))
  (check-type column non-negative-integer)
  (check-type row non-negative-integer))
