(in-package #:vellum)


(defun empty-column (header-class &rest row-parameters)
  (~>> (make-header header-class row-parameters)
       (vellum.table:make-table 'vellum.table:standard-table)))


(defun new-columns (table &rest columns)
  (~>> table
       vellum.table:header
       class-of
       (vellum:make-header _ columns)
       (vellum.table:make-table (class-of table))
       (vstack table)))


(defun empty-table (&key (header (vellum.header:header)))
  (vellum.table:make-table 'vellum.table:standard-table header))


(defun order-by (table column comparator &rest columns-comparators)
  (let* ((content (make-array (row-count table)))
         (header (vellum.table:header table))
         (comparators (~>> (batches columns-comparators 2)
                           (mapcar #'second)
                           (cons comparator)
                           nreverse))
         (indexes (~>> (batches columns-comparators 2)
                       (mapcar #'first)
                       (cons column)
                       (mapcar (lambda (x)
                                 (check-type x (or symbol
                                                   string
                                                   non-negative-integer))
                                 (if (or (symbolp x)
                                         (stringp x))
                                     (vellum.header:alias-to-index header x)
                                     x)))
                       nreverse))
         (i 0))
    (transform table
               (lambda (&rest all)
                 (declare (ignore all))
                 (setf (aref content i) (vellum.header:current-row-as-vector))
                 (incf i))
               :in-place t)
    (iterate
      (for index in indexes)
      (for comparator in comparators)
      (setf content
            (stable-sort content comparator
                         :key (lambda (v) (aref v index)))))
    (to-table content :header header)))


(defun collect-column-specs (frame-specs)
  (bind ((raw-column-specs
          (iterate
            (for (label frame column) in frame-specs)
            (for header = (vellum.table:header frame))
            (for column-specs = (vellum.header:column-specs header))
            (for transformed-specs =
                 (mapcar (lambda (x)
                           (let* ((alias (getf x :alias))
                                  (new-alias (format nil "~a-~a" alias alias)))
                             (list :alias new-alias
                                   :predicate (getf x :predicate)
                                   :type (getf x :type))))
                         column-specs))
            (adjoining transformed-specs))))
    raw-column-specs))


(defun cartesian-product (vector)
  (iterate
    (with length = (length vector))
    (with lengths = (map 'vector #'length vector))
    (with total-size = (reduce #'* vector :key #'length))
    (with result = (make-array total-size))
    (with indexes = (make-array (length vector)
                                :initial-element 0))
    (for i from 0 below total-size)
    (setf (aref result i) (map 'vector #'aref vector indexes))
    (iterate
      (for i from 0 below length)
      (for index = (1+ (aref indexes i)))
      (for l = (aref lengths i))
      (if (= index l)
          (setf (aref indexes i) 0)
          (progn
            (setf (aref indexes i) index)
            (leave))))
    (finally (return (cl-ds:whole-range result)))))


(defun hash-join-implementation (frame-specs header class test function)
  (let ((frames-count (length frame-specs))
        (hash-table (make-hash-table :test test))
        (fresh-table (vellum.table:make-table class header)))
    (iterate
      (for i from 0)
      (for (label frame column) in frame-specs)
      (for column-count = (vellum:column-count frame))
      (vellum:transform frame
                       (lambda (&rest all)
                         (declare (ignore all))
                         (let* ((row (vellum.header:row))
                                (key (vellum:rr column row)))
                           (unless (null key)
                             (let ((data
                                     (ensure (gethash key hash-table)
                                       (map-into (make-array frames-count)
                                                 #'vect)))
                                   (row-data (make-array column-count)))
                               (iterate
                                 (for i from 0 below column-count)
                                 (setf (aref row-data i) (vellum:rr i row)))
                               (vector-push-extend row-data (aref data i))))))
                       :in-place t))
  (let ((range (~> hash-table
                   cl-ds.alg:make-hash-table-range
                   (cl-ds.alg:multiplex :function function
                                        :key #'cdr))))
    (vellum:transform fresh-table
                     (lambda (&rest all)
                       (declare (ignore all))
                       (let ((row-data (cl-ds:consume-front range)))
                         (if (null row-data)
                             (vellum:finish-transformation)
                             (iterate
                               (with k = 0)
                               (for i from 0 below (length row-data))
                               (for sub = (aref row-data i))
                               (iterate
                                 (for j from 0 below (length sub))
                                 (setf (vellum:rr k) (aref sub j))
                                 (incf k))))))
                     :in-place t
                     :end nil))))


(defmethod join ((algorithm (eql :hash)) (method (eql :inner)) (frame-specs list)
                 &key
                   (class 'vellum.table:standard-table)
                   (header-class 'vellum.header:standard-header)
                   (columns (collect-column-specs frame-specs))
                   (header (apply #'vellum.header:make-header
                                  header-class columns))
                   (test 'eql))
  (hash-join-implementation frame-specs header
                            class test
                            #'cartesian-product))


(defmethod join ((algorithm (eql :hash)) (method (eql :left)) (frame-specs list)
                 &key
                   (class 'vellum.table:standard-table)
                   (header-class 'vellum.header:standard-header)
                   (columns (collect-column-specs frame-specs))
                   (header (apply #'vellum.header:make-header
                                  header-class columns))
                   (test 'eql))
  (bind ((lengths (map 'vector
                       (compose #'vellum:column-count #'second)
                       frame-specs))
         (length (length frame-specs))
         ((:flet join-product (input))
          (cond ((emptyp (aref input 0))
                 (cl-ds:whole-range '()))
                ((some #'emptyp input)
                 (iterate
                   (with first-length = (~> input (aref 0) length))
                   (with result = (make-array first-length))
                   (for i from 0 below first-length)
                   (for data = (make-array length))
                   (iterate
                     (for j from 1 below length)
                     (setf (aref data j) (make-array (aref lengths j)
                                                     :initial-element :null)))
                   (setf (aref data 0) (~> input (aref 0) (aref i))
                         (aref result i) data)
                   (finally (return (cl-ds:whole-range result)))))
                (t (cartesian-product input)))))
    (hash-join-implementation frame-specs header
                              class test
                              #'join-product)))


(defun add-columns (frame &rest column-specs)
  (vellum:hstack frame
                (mapcar (lambda (x)
                          (apply #'vellum:empty-column
                                 (~> frame vellum.table:header class-of)
                                 x))
                        column-specs)))


(defun to-matrix (frame &key (element-type t) (key #'identity))
  (let* ((column-count (column-count frame))
         (row-count (row-count frame))
         (row-index 0)
         (result (make-array (list row-count column-count)
                             :element-type element-type)))
    (transform frame
               (body ()
                 (iterate
                   (with row = (vellum.header:row))
                   (for i from 0 below column-count)
                   (setf (aref result row-index i)
                         (coerce (funcall key (rr i row))
                                 element-type))
                   (finally (incf row-index)))))
    result))
