(in-package #:cl-df.column)


(defclass fundamental-column ()
  ())


;; (defclass fundamental-observer ()
;;   ())


(defclass fundamental-iterator ()
  ())


(defclass fundamental-pure-iterator (fundamental-iterator)
  ())


(defclass complex-iterator (fundamental-iterator)
  ())


(defclass sparse-material-column (cl-ds.dicts.srrb:transactional-sparse-rrb-vector
                                  fundamental-column)
  ((%column-size :initarg :column-size
                 :reader read-column-size
                 :reader column-size
                 :documentation "Highest index+1 in this column.")))
