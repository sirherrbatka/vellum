(cl:in-package #:vellum)


(defun s-list (table &rest forms)
  (apply #'vellum.selection:s-list
         (vellum.table:header table)
         forms))


(defun %aggregate-rows (table &rest params)
  (bind ((pairs (batches params 2))
         (row-count (row-count table))
         (header (vellum.table:header table))
         ((:flet ensure-name (id))
          (if (numberp id)
              (vellum.header:index-to-name header
                                           id)
              id))
         (ensure-index (curry #'vellum.header:ensure-index header))
         ((:flet materialize-selectors (x))
          (if (typep x 'vellum.selection:selector)
              (~> (vellum.selection:address-range x ensure-index
                                                  (vellum.header:column-count header))
                  cl-ds.alg:to-list)
              x))
         (names (~>> pairs
                     (mapcar (compose #'materialize-selectors #'first))
                     flatten
                     (cl-ds.utils:transform #'ensure-name)))
         (result (vellum.table:make-table
                  :header (apply #'vellum.header:make-header
                                 (mapcar (curry #'list :name)
                                         names)))))
    (iterate
      (for i from 0)
      (for (name (aggregator-constructor . params)) in pairs)
      (setf name (materialize-selectors name))
      (unless (listp name)
        (setf name (list name)))
      (cl-ds.utils:transform #'ensure-name name)
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


(defun empty-column (&rest row-parameters)
  (vellum.table:make-table :header (make-header row-parameters)
                           :columns row-parameters))


(defun column-name= (a b)
  (string= (if (stringp a) a (symbol-name a))
           (if (stringp b) b (symbol-name b))))


(defun unnest (table column &rest more-columns)
  (bind ((all-unnested-columns (cons column more-columns))
         (old-header (vellum.table:header table))
         (old-columns (vellum.header:column-specs old-header))
         (columns-count (length old-columns))
         (unnest-columns-hash-table
          (cl-ds.alg:to-hash-table all-unnested-columns
                                   :test 'eql
                                   :key (lambda (name)
                                          (if (integerp name)
                                              name
                                              (vellum.header:name-to-index old-header name)))))
         (result (vellum.table:make-table
                  :columns (iterate
                             (for i from 0)
                             (for signature in old-columns)
                             (if (gethash (getf signature :name i) unnest-columns-hash-table)
                                 (let ((result (copy-list signature)))
                                   (setf (getf result :type) t)
                                   (collecting result))
                                 (collecting signature)))))
         (column-values (make-array (length old-columns)))
         (row-count (vellum:row-count table))
         (current-row -1)
         (from-transformation
          (vellum.table:transformation table
                                       (lambda (&rest ignored) (declare (ignore ignored))
                                         (incf current-row)
                                         (when (< current-row row-count)
                                           (iterate
                                             (for i from 0 below columns-count)
                                             (setf (aref column-values i)
                                                   (if (gethash i unnest-columns-hash-table)
                                                       (cl-ds:whole-range (vellum:rr i))
                                                       (vellum:rr i))))))
                                       :in-place t
                                       :restarts-enabled nil))
         (into-transformation
          (vellum.table:transformation result
                                      nil
                                       :in-place t
                                       :restarts-enabled nil)))
    (vellum.table:transform-row from-transformation)
    (iterate
      (while (< current-row row-count))
      (block sub
        (vellum.table:transform-row into-transformation
                                    (lambda (&rest ignored) (declare (ignore ignored))
                                      (iterate
                                        (with filledp = nil)
                                        (for i from 0 below columns-count)
                                        (for unnestedp = (gethash i unnest-columns-hash-table))
                                        (if unnestedp
                                            (bind (((:values value more) (cl-ds:consume-front (aref column-values i))))
                                              (when more
                                                (setf filledp t
                                                      (vellum:rr i) value)))
                                            (setf (vellum:rr i) (aref column-values i)))
                                        (finally (unless filledp
                                                   (vellum.table:transform-row from-transformation)
                                                   (vellum.table:nullify)
                                                   (return-from sub))))))))
    (vellum.table:transformation-result into-transformation)))


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
                                     (vellum.header:name-to-index header x)
                                     x)))
                       nreverse))
         (i 0))
    (transform table
               (vellum:bind-row ()
                 (setf (aref content i) (vellum.table:current-row-as-vector))
                 (incf i))
               :in-place t
               :restarts-enabled nil)
    (iterate
      (for index in indexes)
      (for comparator in comparators)
      (setf content
            (stable-sort content comparator
                         :key (lambda (v) (aref v index)))))
    (to-table (cl-ds:whole-range content) :header header)))


(defun collect-column-specs (frame-specs)
  (iterate outer
    (declare (ignorable columns))
    (for (label frame . columns) in frame-specs)
    (for header = (vellum.table:header frame))
    (for column-specs = (vellum.header:column-specs header))
    (iterate
      (for x in column-specs)
      (for name = (getf x :name))
      (for new-name = (if (null label)
                          name
                          (format nil "~a/~a" label name)))
      (in outer (collecting
                  (list :name new-name
                        :type (getf x :type)))))))


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
  (bind ((frames-count (length frame-specs))
         (hash-table (make-hash-table :test test))
         (fresh-table (vellum.table:make-table :class class :header header))
         ((:flet key-values (columns))
          (if (endp (rest columns))
              (let ((column (first columns)))
                (lambda (row)
                  (vellum:rr column row)))
              (lambda (row)
                (mapcar (rcurry #'vellum:rr row)
                        columns)))))
    (iterate
      (declare (ignorable label))
      (for i from 0)
      (for (label frame . columns) in frame-specs)
      (for column-count = (vellum:column-count frame))
      (for selector = (key-values columns))
      (vellum:transform frame
                       (vellum:bind-row ()
                         (let* ((row (vellum.header:row))
                                (key (funcall selector row)))
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
                        (vellum:bind-row ()
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
                   (columns (collect-column-specs frame-specs))
                   (header (apply #'vellum.header:make-header columns))
                   (test 'equal))
  (hash-join-implementation frame-specs header
                            class test
                            #'cartesian-product))


(defmethod join ((algorithm (eql :hash)) (method (eql :left)) (frame-specs list)
                 &key
                   (class 'vellum.table:standard-table)
                   (columns (collect-column-specs frame-specs))
                   (header (apply #'vellum.header:make-header columns))
                   (test 'equal))
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
                            (etypecase x
                              (list
                               (apply #'vellum:empty-column
                                      x))
                              ((or string symbol)
                               (vellum:empty-column :name x))))
                          column-specs)))


(defun to-matrix (frame &key (element-type t) (key #'identity))
  (declare (optimize (debug 3)))
  (let* ((column-count (column-count frame))
         (row-count (row-count frame))
         (row-index 0)
         (result (make-array (list row-count column-count)
                             :element-type element-type)))
    (transform frame
               (bind-row ()
                 (iterate
                   (with row = (vellum.header:row))
                   (for i from 0 below column-count)
                   (setf (aref result row-index i)
                         (coerce (funcall key (rr i row))
                                 element-type))
                   (finally (incf row-index)))))
    result))


(defun %aggregate-columns (table aggregator-constructor
                           &key (skip-nulls nil) (type t)
                             name)
  (declare (optimize (speed 3)))
  (bind ((column-count (column-count table))
         (result (vellum.table:make-table
                  :header (vellum.header:make-header
                           `(:name ,name
                             :type ,type)))))
    (declare (type fixnum column-count))
    (~> (transform (hstack* table (list result) :isolate nil)
                   (vellum:bind-row ()
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
        (select :columns `(,column-count)))))


(defun rename-columns (table old-name new-name &rest more-names)
  (bind ((new-names-vector (~> (list* old-name new-name more-names)
                               (cl-ds.alg:on-each
                                (lambda (string/symbol/number)
                                  (etypecase string/symbol/number
                                    (string string/symbol/number)
                                    (symbol (symbol-name string/symbol/number))
                                    (non-negative-fixnum string/symbol/number))))
                               (cl-ds.alg:in-batches 2)
                               cl-ds.alg:to-list))
         (old-column-names (coerce (column-names table) 'vector))
         (used (make-array (length new-names-vector) :initial-element nil))
         (new-column-names (copy-array old-column-names))
         ((:labels find-new-name (index))
          (let* ((old-name (aref old-column-names index))
                 (position (or (position old-name new-names-vector
                                         :test 'equal :key #'first)
                               (position index new-names-vector
                                         :test 'equal :key #'first))))
            (if (null position)
                (aref old-column-names index)
                (progn
                  (setf (aref used position) t)
                  (second (aref new-names-vector position)))))))
    (iterate
      (for index from 0 below (length old-column-names))
      (for column-name = (aref old-column-names index))
      (cond ((null column-name)
             (setf (aref old-column-names index) index))
            ((symbolp column-name)
             (setf (aref old-column-names index) (symbol-name column-name)))
            ((stringp column-name) t))
      (for new-name = (find-new-name index))
      (setf (aref new-column-names index) new-name))
    (unless (every #'identity used)
      (error "Name mappings ~a not applied."
             (~>> (map 'list #'list used new-names-vector)
                  (remove-if #'first)
                  (mapcar #'second))))
    (vellum:select table
      :columns (map 'list #'list old-column-names new-column-names))))


(defun some-null-column-p* (&optional
                              (row (vellum.header:row))
                              (header vellum.header:*header*))
  "Returns true if there is at least one :NULL column in the current row."
  (iterate
    (for i from 0 below (vellum.header:column-count header))
    (when (eq :null (vellum.table:row-at header row i))
      (leave t))
    (finally (return nil))))


(defun every-null-column-p* (&optional
                               (row (vellum.header:row))
                               (header vellum.header:*header*))
  "Returns true if there if all columns within the current row are :NULL."
  (iterate
    (for i from 0 below (vellum.header:column-count header))
    (constantly (eq :null (vellum.table:row-at header row i)))))
