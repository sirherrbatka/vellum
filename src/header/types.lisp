(in-package #:cl-data-frames.header)


(defclass fundamental-header ()
  ())


(defclass standard-header ()
  ((%column-aliases :type hash-table
                    :initarg :column-aliases
                    :reader read-column-aliases)
   (%column-types :type simple-vector
                  :initarg :column-types
                  :reader read-column-types)))


(defclass fundamental-row ()
  ())


(defclass standard-row (fundamental-row)
  ((%header :type fundamental-header
            :initarg :header
            :reader read-header)
   (%content :type 'simple-vector
             :initarg :content
             :reader read-content)))
