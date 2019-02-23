(in-package #:cl-data-frames.header)


(defclass fundamental-header ()
  ())


(defclass standard-header ()
  ((%column-names :type hash-table
                  :initarg :column-names
                  :initform (make-hash-table :test 'eql
                                             :size 128))))
