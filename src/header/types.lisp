(cl:in-package #:vellum.header)


(defclass fundamental-header ()
  ())


(def constantly-t (constantly t))


(defclass column-signature ()
  ((%type :initarg :type
          :reader read-type)
   (%predicate :initarg :predicate
               :reader read-predicate)
   (%name :initarg :name
          :reader read-name))
  (:default-initargs :type t
                     :name nil
                     :predicate constantly-t))


(defclass standard-header (fundamental-header)
  ((%column-signature-class :initarg :column-signature-class
                            :initform 'column-signature
                            :reader read-column-signature-class)
   (%column-signatures :initarg :column-signatures
                       :reader read-column-signatures)
   (%column-names :type hash-table
                  :initarg :column-names
                  :reader read-column-names)))


(defmethod cl-ds.utils:cloning-information append
    ((header standard-header))
  '((:column-signature-class read-column-signature-class)
    (:column-signatures read-column-signatures)
    (:column-names read-column-names)))


(defmethod cl-ds.utils:cloning-information append
    ((signature column-signature))
  '((:type read-type)
    (:name read-name)
    (:predicate read-predicate)))


(defclass frame-range-mixin ()
  ((%list-format :initarg :list-format
                 :reader read-list-format))
  (:default-initargs :list-format nil))


(defclass validated-frame-range-mixin (frame-range-mixin)
  ())


(defclass forward-proxy-frame-range (frame-range-mixin
                                     cl-ds.alg:forward-proxy-range)
  ())


(defmethod cl-ds.utils:cloning-information append
    ((signature frame-range-mixin))
  '((:list-format read-list-format)))


(defclass bind-row ()
  ((%optimized-closure :initarg :optimized-closure
                       :reader optimized-closure))
  (:metaclass closer-mop:funcallable-standard-class))
