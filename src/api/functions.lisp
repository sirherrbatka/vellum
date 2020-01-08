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
