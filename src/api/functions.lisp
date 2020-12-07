(cl:in-package #:vellum)


(defun empty-column (header-class &rest row-parameters)
  (vellum.table:make-table :header (make-header header-class row-parameters)
                           :columns row-parameters))


(defun new-columns (table &rest columns)
  (~>> table
       vellum.table:header
       type-of
       (apply #'vellum:make-header _ columns)
       (vellum.table:make-table (type-of table))
       list
       (hstack* table)))


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
  (vellum:hstack* frame
                  (mapcar (lambda (x)
                            (apply #'vellum:empty-column
                                   (~> frame vellum.table:header class-of)
                                   x))
                          column-specs)))


(defun to-matrix (frame &key (element-type t) (key #'identity))
  (declare (optimize (debug 3)))
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


(defun alter-columns (table column params &rest more)
  (bind ((header (vellum.table:header table))
         (pairs (cons (list column params)
                      (batches more 2)))
         (columns-count (vellum.header:column-count header))
         ((:flet alter-column (index))
          (let* ((column (vellum.header:column-signature header index))
                 (id (or (vellum.header:read-alias column)
                         index))
                 (new-params (find-if (lambda (x)
                                        (or (equal id x)
                                            (eql index x)))
                                      pairs
                                      :key #'first)))
            (if (null new-params)
                (cl-ds.utils:cloning-list column)
                (append (second new-params)
                        (cl-ds.utils:cloning-list column)))))
         (new-header (apply #'vellum.header:make-header
                            (class-of header)
                            (iterate
                              (for i from 0 below columns-count)
                              (collect (alter-column i)))))
         (result (cl-ds.utils:quasi-clone table
                                          :header new-header)))
    (replica result t)))


(defun %aggregate-rows (table &rest params)
  (bind ((pairs (batches params 2))
         (row-count (row-count table))
         (names (flatten (mapcar #'first pairs)))
         (result (vellum.table:make-table
                  :header (apply #'vellum.header:make-header
                                 'vellum.header:standard-header
                                 (mapcar (curry #'list :alias)
                                         names)))))
    (iterate
      (for i from 0)
      (for (name (aggregator-constructor . params)) in pairs)
      (unless (listp name)
        (setf name (list name)))
      (iterate
        (for id in name)
        (for column = (vellum.table:column-at table id))
        (for aggregator = (funcall aggregator-constructor))
        (if (getf params :skip-nulls)
            (cl-ds.alg.meta:across-aggregate
             column
             (curry #'cl-ds.alg.meta:pass-to-aggregation aggregator))
            (iterate
              (for i from 0 below row-count)
              (cl-ds.alg.meta:pass-to-aggregation
               aggregator
               (vellum.column:column-at column i))))
        (setf (at result 0 id)
              (cl-ds.alg.meta:extract-result aggregator)))
      (finally (return result)))))


(defun %aggregate-columns (table aggregator-constructor
                           &key (skip-nulls nil) (type t)
                           alias
                             (predicate vellum.header:constantly-t))
  (declare (optimize (speed 3)))
  (bind ((column-count (column-count table))
         (result (vellum.table:make-table
                  :header (vellum.header:make-header
                           'vellum.header:standard-header
                           `(:predicate ,predicate
                             :alias ,alias
                             :type ,type)))))
    (declare (type fixnum column-count))
    (~> (transform (hstack* table (list result) :isolate nil)
                     (lambda (&rest _) (declare (ignore _))
                       (cl-ds.utils:cases ((null skip-nulls))
                         (iterate
                           (declare (type fixnum i))
                           (with aggregator = (funcall aggregator-constructor))
                           (for i from 0 below column-count)
                           (for value = (rr i))
                           (when (and skip-nulls (eq :null value))
                             (next-iteration))
                           (cl-ds.alg.meta:pass-to-aggregation aggregator value)
                           (finally (setf (rr column-count)
                                          (cl-ds.alg.meta:extract-result aggregator))))))
                     :in-place nil)
        (select :columns `(:v ,column-count)))))
