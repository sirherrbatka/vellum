(cl:defpackage #:mushrooms-example
  (:use #:cl))

(ql:quickload '(:vellum :vellum-csv))
(cl:in-package #:mushrooms-example)

#|
Mushrooms data set is just a simple set for writing classification ML models. This example implements a very basic one-hot encoder.
We will assume that all attributes are discreete. This is not far from the truth and makes the example less hairy.
|#
(defparameter *source-data*
  (vellum:copy-from :csv (asdf:system-relative-pathname :vellum "examples/mushrooms.data")
                    :includes-header-p nil
                    :columns '(class ; this is actually the target for the classification so it is largely ignored
                               cap-shape
                               cap-surface
                               cap-color
                               bruises?
                               odor
                               gill-attachment
                               gill-spacing
                               gill-size
                               gill-color
                               stalk-shape
                               stalk-root
                               stalk-surface-above-ring
                               stalk-surface-below-ring
                               stalk-color-above-ring
                               stalk-color-below-ring
                               veil-type
                               veil-color
                               ring-number
                               ring-type
                               spore-print-color
                               population
                               habitat)))


(defparameter *train-data-columns*
  (vellum:with-table (*source-data*)
    (vellum:between :from 'cap-shape))) ; everything but the CLASS


#|
Classes in this data frame are represented by short (one character long) strings.
This code gathers all of those strings, arranges it in a array and enumerates it using hash-table.
|#
(defparameter *encoder-dicts*
  (vellum:pipeline (*source-data*)
    (cl-ds.alg:on-each (vellum:bind-row ()
                         (cl-ds.alg:to-vector (vellum:vs *train-data-columns*))))
    cl-ds.alg:array-elementwise
    (cl-ds.alg:enumerate :test 'equal)))


#|
We need a way to translate column classes into position in the encoded matrix.
To do so we need to gather classes count.
|#
(defparameter *encoder-offsets*
  (serapeum:scan #'+ *encoder-dicts*
                 :key #'hash-table-count
                 :initial-value 0))


#|
Wrap the offset and the classes dict into one object.
|#
(defclass one-hot-encoder ()
  ((%offset :initarg :offset
            :reader offset)
   (%dict :initarg :dict
          :reader dict)))


(defun make-one-hot-encoder (dict offset)
  (make-instance 'one-hot-encoder
                 :offset offset
                 :dict dict))


(defparameter *encoders*
  (map 'vector
       #'make-one-hot-encoder
       *encoder-dicts*
       *encoder-offsets*))


#|
What is the number of columns in the result matrix?
|#
(defparameter *encoded-size*
  (reduce #'+ *encoder-dicts* :key #'hash-table-count))


#|
Actual implementation of the encoder.
|#
(defun encode-value (encoder row matrix value)
  (setf (aref matrix row (+ (offset encoder) (gethash value (dict encoder))))
        1.0))


(defparameter *encoded*
  (make-array (list (vellum:row-count *source-data*) *encoded-size*)
              :element-type 'single-float
              :initial-element 0.0))


(vellum:transform *source-data*
                  (vellum:bind-row ()
                    (loop :for value :across (cl-ds.alg:to-vector (vellum:vs *train-data-columns*))
                          :for encoder :across *encoders*
                          :do (encode-value encoder vellum:*current-row* *encoded* value))))
