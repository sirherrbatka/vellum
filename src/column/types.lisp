(cl:in-package #:vellum.column)


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


(deftype iterator-change ()
  `(simple-array boolean (,cl-ds.common.rrb:+maximum-children-count+)))


(deftype iterator-stack ()
  `(simple-vector ,cl-ds.common.rrb:+maximal-shift+))


(deftype iterator-buffer ()
  `(simple-vector ,cl-ds.common.rrb:+maximum-children-count+))


(defstruct sparse-material-column-iterator
  (initialization-status (make-array 0 :element-type 'boolean)
   :type (simple-array boolean (*)))
  transformation
  (columns #() :type simple-vector)
  (stacks #() :type simple-vector)
  (depths (make-array 0 :element-type 'fixnum)
   :type (simple-array fixnum (*)))
  (index 0 :type fixnum)
  (initial-index 0 :type fixnum)
  (touched (make-array 0 :element-type 'boolean) :type (simple-array boolean (*)))
  (buffers #() :type simple-vector)
  (changes #() :type simple-vector))

(declaim (inline read-transformation))
(declaim (inline read-initialization-status))
(declaim (inline index))
(declaim (inline read-columns))
(declaim (inline read-stacks))
(declaim (inline read-depths))
(declaim (inline read-initial-index))
(declaim (inline read-touched))
(declaim (inline read-buffers))
(declaim (inline read-changes))
(declaim (inline access-index))
(declaim (inline (setf access-index)))
(declaim (inline columns))

(defun read-initialization-status (iterator)
  (sparse-material-column-iterator-initialization-status iterator))

(defun read-transformation (iterator)
  (sparse-material-column-iterator-transformation iterator))

(defun index (iterator)
  (sparse-material-column-iterator-index iterator))

(defun read-columns (iterator)
  (sparse-material-column-iterator-columns iterator))

(defun read-stacks (iterator)
  (sparse-material-column-iterator-stacks iterator))

(defun read-depths (iterator)
  (sparse-material-column-iterator-depths iterator))

(defun read-initial-index (iterator)
  (sparse-material-column-iterator-initial-index iterator))

(defun read-touched (iterator)
  (sparse-material-column-iterator-touched iterator))

(defun read-buffers (iterator)
  (sparse-material-column-iterator-buffers iterator))

(defun read-changes (iterator)
  (sparse-material-column-iterator-changes iterator))

(defun access-index (iterator)
  (sparse-material-column-iterator-index iterator))

(defun (setf access-index) (new-value iterator)
  (setf (sparse-material-column-iterator-index iterator) new-value))

(defun columns (iterator)
  (sparse-material-column-iterator-columns iterator))
