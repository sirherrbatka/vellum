(in-package #:cl-df.column)


(defmethod augment-iterator :around ((iterator (eql nil))
                                     (column fundamental-column))
  (make-iterator column))
