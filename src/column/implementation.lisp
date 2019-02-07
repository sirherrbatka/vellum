(in-package #:cl-df.column)


(defmethod augment-iterator :around ((iterator (eql nil))
                                     (column fundamental-column))
  (make-iterator column))


(defmethod column-type ((column sparse-material-column))
  (cl-ds:type-specialization column))


(defmethod column-type ((column fundamental-iterator))
  t)
