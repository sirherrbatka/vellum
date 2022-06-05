(cl:in-package #:vellum.table)


(defclass fundamental-table (cl-ds:traversable)
  ())


(defclass standard-table (fundamental-table)
  ((%header :reader header
            :initarg :header)
   (%columns :reader read-columns
             :writer write-columns
             :initarg :columns
             :type vector)))


(defstruct standard-transformation
  (dropped nil :type boolean)
  marker-column
  table
  (restarts-enabled t :type boolean)
  (in-place nil :type boolean)
  (start 0 :type integer)
  row
  iterator
  (columns #() :type simple-vector)
  (column-count 0 :type fixnum)
  (count 0 :type fixnum)
  bind-row-closure
  aggregation-results)


(cl-ds.utils:define-list-of-slots standard-transformation ()
  (column-count standard-transformation-column-count)
  (dropped standard-transformation-dropped)
  (marker-column standard-transformation-marker-column)
  (restarts-enabled standard-transformation-restarts-enabled)
  (table standard-transformation-table)
  (in-place standard-transformation-in-place)
  (start standard-transformation-start)
  (row standard-transformation-row)
  (iterator standard-transformation-iterator)
  (columns standard-transformation-columns)
  (aggregation-results standard-transformation-aggregation-results)
  (count standard-transformation-count))


(defmethod cl-ds.utils:cloning-information append ((table standard-table))
  '((:header header)
    (:columns read-columns)))


(defstruct table-row iterator)
(declaim (inline setfable-table-row-iterator))
(declaim (inline table-row-iterator))
(defstruct (setfable-table-row (:include table-row)))


(defclass standard-table-range (cl-ds:fundamental-forward-range)
  ((%table-row :initarg :table-row
               :reader read-table-row)
   (%header :initarg :header
            :reader read-header
            :reader header)
   (%row-count :initarg :row-count
               :type fixnum
               :reader read-row-count)))


(defmethod cl-ds.utils:cloning-information append
    ((range standard-table-range))
  '((:table-row read-table-row)
    (:header read-header)
    (:row-count read-row-count)))


(defclass bind-row ()
  ((%optimized-closure :initarg :optimized-closure
                       :reader optimized-closure))
  (:metaclass closer-mop:funcallable-standard-class))
