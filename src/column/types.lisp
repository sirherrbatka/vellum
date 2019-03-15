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
  ((%columns :initarg :columns
             :type vector
             :initform (vect)
             :reader read-columns)
   (%stacks :initarg :stacks
            :type vector
            :initform (vect)
            :reader read-stacks)
   (%depths :initarg :depths
            :type (vector fixnum)
            :initform (make-array 0 :element-type 'fixnum
                                    :adjustable t
                                    :fill-pointer 0)
            :reader read-depths)
   (%index :initarg :index
           :accessor access-index
           :type fixnum
           :initform 0)
   (%buffers :initarg :buffers
             :type vector
             :initform (vect)
             :reader read-buffers)
   (%changes :initarg :changes
             :initform (vect)
             :type vector
             :reader read-changes)))


(defmethod cl-ds.utils:cloning-information append
    ((iterator sparse-material-column-iterator))
  '((:columns read-columns)
    (:stacks read-stacks)
    (:depths read-depths)
    (:index access-index)
    (:buffers read-buffers)
    (:changes read-changes)))


(defmethod cl-ds:clone ((iterator sparse-material-column-iterator))
  (apply #'make (class-of iterator)
         :columns (~> iterator read-columns copy-array)
         :stacks (~>> iterator read-stacks (map 'vector #'copy-array))
         :depths (~> iterator read-depths copy-array)
         :index (access-index iterator)
         :buffers (~> iterator read-buffers copy-array)
         :changes (~> iterator read-changes copy-array)
         (cl-ds.utils:cloning-information iterator)))
