(in-package #:cl-df)


(defun empty-column (header-class &rest row-parameters)
  (~>> (make-header header-class row-parameters)
       (cl-df.table:make-table 'cl-df.table:standard-table)))


(defun new-columns (table &rest columns)
  (~>> table
       cl-df.table:header
       class-of
       (cl-df:make-header _ columns)
       (cl-df.table:make-table (class-of table))
       (vstack table)))


(defun sample (table chance-to-pick &key (in-place *transform-in-place*))
  (check-type chance-to-pick (real 0 1))
  (transform table
             (lambda (&rest all)
               (declare (ignore all))
               (unless (< chance-to-pick (random 1.0d0))
                 (drop-row)))
             :in-place in-place))


(defun empty-table (&key (header (cl-df.header:header)))
  (cl-df.table:make-table 'cl-df.table:standard-table header))


(defun order-by (table column comparator &rest columns-comparators)
  (let* ((content (make-array (row-count table)))
         (header (cl-df.table:header table))
         (comparators (~>> (batches columns-comparators 2)
                           (mapcar #'second)
                           (cons comparator)
                           nreverse))
         (indexes (~>> (batches columns-comparators 2)
                       (mapcar #'first)
                       (cons column)
                       (mapcar (lambda (x)
                                 (cl-df.header:alias-to-index header x)))
                       nreverse))
         (i 0))
    (transform table
               (lambda (&rest all)
                 (declare (ignore all))
                 (setf (aref content i)
                       (cl-ds.alg:to-vector (cl-df.header:row)))
                 (incf i))
               :in-place nil)
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
            (for header = (cl-df.table:header frame))
            (for column-specs = (cl-df.header:column-specs header))
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
        (fresh-table (cl-df.table:make-table class header)))
    (iterate
      (for i from 0)
      (for (label frame column) in frame-specs)
      (for column-count = (cl-df:column-count frame))
      (cl-df:transform frame
                       (lambda (&rest all)
                         (declare (ignore all))
                         (let* ((row (cl-df.header:row))
                                (key (cl-df:rr column row)))
                           (unless (null key)
                             (let ((data
                                     (ensure (gethash key hash-table)
                                       (map-into (make-array frames-count)
                                                 #'vect)))
                                   (row-data (make-array column-count)))
                               (iterate
                                 (for i from 0 below column-count)
                                 (setf (aref row-data i) (cl-df:rr i row)))
                               (vector-push-extend row-data (aref data i))))))
                       :in-place t))
  (let ((range (~> hash-table
                   cl-ds.alg:make-hash-table-range
                   (cl-ds.alg:multiplex :function function
                                        :key #'cdr))))
    (cl-df:transform fresh-table
                     (lambda (&rest all)
                       (declare (ignore all))
                       (let ((row-data (cl-ds:consume-front range)))
                         (if (null row-data)
                             (cl-df:finish-transformation)
                             (iterate
                               (with k = 0)
                               (for i from 0 below (length row-data))
                               (for sub = (aref row-data i))
                               (iterate
                                 (for j from 0 below (length sub))
                                 (setf (cl-df:rr k) (aref sub j))
                                 (incf k))))))
                     :in-place t
                     :end nil))))


(defmethod join ((algorithm (eql :hash)) (method (eql :inner)) (frame-specs list)
                 &key
                   (class 'cl-df.table:standard-table)
                   (header-class 'cl-df.header:standard-header)
                   (columns (collect-column-specs frame-specs))
                   (header (apply #'cl-df.header:make-header
                                  header-class columns))
                   (test 'eql))
  (hash-join-implementation frame-specs header class test #'cartesian-product))


(defmethod join ((algorithm (eql :hash)) (method (eql :left)) (frame-specs list)
                 &key
                   (class 'cl-df.table:standard-table)
                   (header-class 'cl-df.header:standard-header)
                   (columns (collect-column-specs frame-specs))
                   (header (apply #'cl-df.header:make-header
                                  header-class columns))
                   (test 'eql))
  (bind ((lengths (map 'vector
                      (compose #'cl-df:column-count #'second)
                      frame-specs))
         (length (length frame-specs))
         (first-length (aref lengths 0))
         ((:flet join-product (input))
          (cond ((emptyp (aref input 0))
                 (cl-ds:whole-range '()))
                ((some #'emptyp input)
                 (let ((result (make-array first-length)))
                   (iterate
                     (for i from 0 below first-length)
                     (for data = (make-array length))
                     (iterate
                       (for j from 1 below length)
                       (setf (aref data j) (make-array (aref lengths j)
                                                       :initial-element :null)))
                     (setf (aref data 0) (~> input
                                             (aref 0)
                                             (aref i)))
                     (setf (aref result i) data))
                   (cl-ds:whole-range result)))
                (t (cartesian-product input)))))
    (hash-join-implementation frame-specs header class test
                              #'join-product)))
