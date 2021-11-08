(cl:in-package #:vellum.header)


(defun constantly-t (&rest ignored)
  (declare (ignore ignored))
  t)


(defclass column-signature ()
  ((%type :initarg :type
          :reader read-type)
   (%predicate :initarg :predicate
               :reader read-predicate)
   (%name :initarg :name
          :reader read-name))
  (:default-initargs :type t
                     :name nil
                     :predicate 'constantly-t))


(defstruct standard-header
  column-signatures
  (column-names (make-hash-table :test 'equal) :type hash-table))


(defun read-column-signatures (header)
  (standard-header-column-signatures header))


(defun read-column-names (header)
  (standard-header-column-names header))


(defmethod cl-ds.utils:cloning-information append
    ((header standard-header))
  '((:column-signatures standard-header-column-signatures)
    (:column-names standard-header-column-names)))


(defmethod cl-ds.utils:cloning-information append
    ((signature column-signature))
  '((:type read-type)
    (:name read-name)
    (:predicate read-predicate)))


(defclass frame-range-mixin ()
  ((%header :initarg :header
            :reader read-header)))


(defmethod cl-ds.utils:cloning-information append
    ((range frame-range-mixin))
  '((:header read-header)))
