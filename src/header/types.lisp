(in-package #:cl-data-frames.header)


(defclass fundamental-header ()
  ())


(def constantly-t (constantly t))


(defclass column-signature ()
  ((%type :initarg :type
          :reader read-type)
   (%predicate :initarg :predicate
               :reader read-predicate)
   (%alias :initarg :alias
           :reader read-alias))
  (:default-initargs :type t
                     :alias ni
                     :predicate constantly-t))


(defclass standard-header ()
  ((%column-signature-class :initarg :column-signature-class
                            :initform 'column-signature
                            :reader read-column-signature-class)
   (%column-signatures :initarg :column-signatures
                       :reader read-column-signatures)
   (%column-aliases :type hash-table
                    :initarg :column-aliases
                    :reader read-column-aliases)))


(defmethod cl-ds.utils:cloning-information append
    ((header standard-header))
  '((:column-signature-class read-column-signature-class)
    (:column-signatures read-column-signatures)
    (:column-aliases read-column-aliases)))


(defmethod cl-ds.utils:cloning-information append
    ((signature column-signature))
  '((:type read-type)
    (:predicate read-predicate)))


(defclass frame-range-mixin ()
  ((%header :initarg :header
            :reader read-header)))


(defclass forward-proxy-frame-range (frame-range-mixin
                                     cl-ds.alg:forward-proxy-range)
  ())
