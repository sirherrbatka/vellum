(in-package #:cl-df.table)


(defclass fundamental-table (cl-ds:traversable)
  ())


(defclass standard-table (fundamental-table)
  ((%header :reader header
            :initarg :header)
   (%columns :reader read-columns
             :writer write-columns
             :initarg :columns
             :type vector)))


(defmethod cl-ds.utils:cloning-information append ((table standard-table))
  '((:header header)
    (:column read-columns)))


(defclass table-row ()
  ((%iterator :initarg :iterator
              :reader read-iterator)))


(defclass setfable-table-row (table-row)
  ())


(defclass standard-table-range (cl-ds:fundamental-forward-range)
  ((%table-row :initarg :table-row
               :reader read-table-row)
   (%header :initarg :header
            :reader read-header)
   (%row-count :initarg :row-count
               :type fixnum
               :reader read-row-count)))


(defmethod cl-ds.utils:cloning-information append
    ((range standard-table-range))
  '((:table-row read-table-row)
    (:header read-header)
    (:row-count read-row-count)))
