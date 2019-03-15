(in-package #:cl-df.table)


(defclass fundamental-table ()
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
              :accessor access-iterator)))


(defclass setfable-table-row (table-row)
  ())


(defclass standard-table-range (cl-ds:fundamental-forward-range)
  ((%iterator :initarg :iterator
              :accessor access-iterator)
   (%header :initarg :header
            :reader read-header)
   (%row :initarg :row
         :type fixnum
         :accessor access-row)
   (%row-count :initarg :row-count
               :type fixnum
               :reader read-row-count)
   (%initial-row :initarg :row
                 :type fixnum
                 :reader read-row))
  (:default-initargs :row 0))
