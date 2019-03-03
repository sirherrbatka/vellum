(in-package #:cl-data-frames.header)


(defclass fundamental-header ()
  ())


(defclass standard-header ()
  ((%column-aliases :type hash-table
                    :initarg :column-aliases
                    :reader read-column-aliases)
   (%predicates :type vector
                :initarg :predicates
                :reader read-predicates)
   (%column-types :type simple-vector
                  :initarg :column-types
                  :reader read-column-types)))


(defclass fundamental-row ()
  ())


(defclass frame-range-mixin ()
  ((%header :initarg :header
            :reader read-header)))


(defclass forward-proxy-frame-range (frame-range-mixin
                                     cl-ds.alg:forward-proxy-range)
  ())
