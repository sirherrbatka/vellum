(in-package #:cl-df.column)


(defclass fundamental-column (cl-ds:traversable)
  ())


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


(defclass sparse-material-column-iterator ()
  ((%initialization-status :initarg :initialization-status
                           :type (vector boolean)
                           :initform (make-array 0 :element-type 'boolean
                                                   :adjustable t
                                                   :fill-pointer 0)
                           :reader read-initialization-status)
   (%transformation :initarg :transformation
                    :reader read-transformation)
   (%columns :initarg :columns
             :type vector
             :initform #()
             :accessor access-columns
             :reader columns
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
    (:transformation read-transformation)
    (:buffers read-buffers)
    (:touched read-touched)
    (:initialization-status read-initialization-status)
    (:changes read-changes)))


(deftype iterator-change ()
  `(simple-array boolean (,cl-ds.common.rrb:+maximum-children-count+)))


(deftype iterator-stack ()
  `(simple-vector ,cl-ds.common.rrb:+maximal-shift+))


(deftype iterator-buffer ()
  `(simple-vector ,cl-ds.common.rrb:+maximum-children-count+))
