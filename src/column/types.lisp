(in-package #:cl-df.column)


(defclass fundamental-column (cl-ds:traversable)
  ())


;; (defclass fundamental-observer ()
;;   ())


(defclass fundamental-iterator ()
  ())


(defclass fundamental-pure-iterator (fundamental-iterator)
  ())


(defclass complex-iterator (fundamental-iterator)
  ((%subiterator-types :initarg :subiterator-types
                       :type hash-table
                       :initform (make-hash-table :test 'eql)
                       :reader read-subiterator-types)
   (%subiterators :initarg :subiterators
                  :type vector
                  :initform (vect)
                  :reader read-subiterators)))


(defclass sparse-material-column
    (cl-ds.dicts.srrb:transactional-sparse-rrb-vector
     fundamental-column)
  ())


(defmethod cl-ds.utils:cloning-information append
    ((column sparse-material-column))
  '())


(defun make-sparse-material-column (&key (element-type t))
  (make 'sparse-material-column
        :ownership-tag (cl-ds.common.abstract:make-ownership-tag)
        :element-type element-type))


(defclass sparse-material-column-iterator (fundamental-pure-iterator)
  ((%initialization-status :initarg :initialization-status
                           :type (vector boolean)
                           :initform (make-array 0 :element-type 'boolean
                                                   :adjustable t
                                                   :fill-pointer 0)
                           :reader read-initialization-status)
   (%columns :initarg :columns
             :type vector
             :initform #()
             :accessor access-columns
             :reader read-columns)
   (%stacks :initarg :stacks
            :type vector
            :initform #()
            :accessor access-stacks
            :reader read-stacks)
   (%depths :initarg :depths
            :type (vector fixnum)
            :initform (make-array 0 :element-type 'fixnum)
            :accessor access-depths
            :reader read-depths)
   (%index :initarg :index
           :accessor access-index
           :reader index
           :type fixnum
           :initform 0)
   (%initial-index :initarg :inder
                   :reader read-initial-index
                   :type fixnum
                   :initform 0)
   (%touched :initarg :touched
             :reader read-touched
             :type vector
             :initform (make-array 0 :element-type 'boolean))
   (%buffers :initarg :buffers
             :type vector
             :initform #()
             :accessor access-buffers
             :reader read-buffers)
   (%changes :initarg :changes
             :initform #()
             :type vector
             :accessor access-changes
             :reader read-changes)))


(defmethod cl-ds.utils:cloning-information append
    ((iterator sparse-material-column-iterator))
  '((:columns read-columns)
    (:stacks read-stacks)
    (:depths read-depths)
    (:index access-index)
    (:buffers read-buffers)
    (:touched read-touched)
    (:initialization-status read-initialization-status)
    (:changes read-changes)))
