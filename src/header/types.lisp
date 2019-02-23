(in-package #:cl-data-frames.header)


(defclass fundamental-header ()
  ())


(defclass standard-header ()
  ((%column-aliases :type hash-table
                    :initarg :column-aliases
                    :reader read-column-aliases
                    :initform (make-hash-table :test 'eql
                                               :size 128))
   (%column-types :type simple-vector
                  :initarg :column-types
                  :reader read-column-types)))
