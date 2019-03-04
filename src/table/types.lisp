(in-package #:cl-df.table)


(defclass fundamental-table ()
  ())


(defclass standard-table (fundamental-table)
  ((%header :reader header
            :initarg :header)
   (%columns :reader read-columns
             :initarg :columns
             :type vector)))
