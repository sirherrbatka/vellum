(in-package #:cl-df.table)


(defclass fundamental-table ()
  ())


(defclass standard-table (fundamental-table)
  ((%header :reader header
            :initarg :header)
   (%columns :reader read-columns
             :initarg :columns
             :type vector)))


(defmethod cl-ds.utils:cloning-information append ((table standard-table))
  '((:header header)
    (:column read-columns)))
