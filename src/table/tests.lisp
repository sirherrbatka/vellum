(cl:in-package #:vellum.table)

(prove:plan 42126)

(progn
  (defparameter *test-data* #(#(1 a 5 s)
                              #(2 b 6 s)
                              #(3 c 7 s)))

  (vellum:with-standard-header (nil nil nil nil)
    (defparameter *table*
      (~> *test-data*
          cl-ds:whole-range
          vellum:decorate
          (vellum:to-table :header (vellum:header)))))

  (prove:is (vellum:at *table* 0 0) 1)
  (prove:is (vellum:at *table* 1 0) 2)
  (prove:is (vellum:at *table* 2 0) 3)

  (prove:is (vellum:at *table* 0 1) 'a)
  (prove:is (vellum:at *table* 1 1) 'b)
  (prove:is (vellum:at *table* 2 1) 'c)

  (prove:is (vellum:at *table* 0 2) 5)
  (prove:is (vellum:at *table* 1 2) 6)
  (prove:is (vellum:at *table* 2 2) 7)

  (prove:is (vellum:at *table* 0 3) 's)
  (prove:is (vellum:at *table* 1 3) 's)
  (prove:is (vellum:at *table* 2 3) 's)

  (defparameter *replica*
    (vellum:transform *table*
                     (vellum:body ()
                       (setf (vellum:rr 0) (+ 1 (vellum:rr 0))))
                     :in-place nil))

  (prove:is (vellum:at *table* 0 0) 1)
  (prove:is (vellum:at *table* 1 0) 2)
  (prove:is (vellum:at *table* 2 0) 3)

  (prove:is (vellum:at *replica* 0 0) 2)
  (prove:is (vellum:at *replica* 1 0) 3)
  (prove:is (vellum:at *replica* 2 0) 4)

  (vellum:transform *table*
                   (vellum:body ()
                     (setf (vellum:rr 0) (* 2 (vellum:rr 0))))
                   :in-place t)

  (prove:is (vellum:at *table* 0 0) 2)
  (prove:is (vellum:at *table* 1 0) 4)
  (prove:is (vellum:at *table* 2 0) 6)

  (prove:is (vellum:at *replica* 0 0) 2)
  (prove:is (vellum:at *replica* 1 0) 3)
  (prove:is (vellum:at *replica* 2 0) 4)

  (defparameter *concatenated-table* (vellum:vstack *table*
                                                    (list *replica*)))
  (prove:is (column-count *concatenated-table*) 4)
  (prove:is (row-count *concatenated-table*) 6)

  (prove:is (vellum:at *concatenated-table* 0 0) 2)
  (prove:is (vellum:at *concatenated-table* 1 0) 4)
  (prove:is (vellum:at *concatenated-table* 2 0) 6)
  (prove:is (vellum:at *concatenated-table* 3 0) 2)
  (prove:is (vellum:at *concatenated-table* 4 0) 3)
  (prove:is (vellum:at *concatenated-table* 5 0) 4)

  (defparameter *sub-table* (select *concatenated-table*
                              :rows '(:take-from 1 :take-to 3)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 1 0) 6)
  (prove:is (at *sub-table* 2 0) 2)

  (defparameter *sub-table* (select *concatenated-table*
                              :columns '(:take-to 2)))
  (prove:is (column-count *sub-table*) 3)
  (prove:is (row-count *sub-table*) 6)

  (defparameter *sub-table* (select *concatenated-table*
                              :rows '(:v 1 :v 2 :v 3)))
  (prove:is (column-count *sub-table*) 4)
  (prove:is (row-count *sub-table*) 3)
  (prove:is (at *sub-table* 0 0) 4)
  (prove:is (at *sub-table* 1 0) 6)
  (prove:is (at *sub-table* 2 0) 2))

(let* ((element-count 252529)
       (source (~> (make-array element-count)
                   (map-into (lambda ()
                               (vector (random most-positive-fixnum)
                                       (random most-positive-fixnum))))))
       (pairs (cl-ds.alg:to-hash-table source
                                       :hash-table-key #'first-elt
                                       :hash-table-value (rcurry #'aref 1)))
       (table (vellum:to-table source
                               :columns '((:alias first-column)
                                          (:alias second-column))))
       (first-even-count (count-if #'evenp source :key #'first-elt))
       (second-even-count (count-if #'evenp source :key (rcurry #'aref 1)))
       (frame-first-even-count 0)
       (null-counts 0)
       (mismatch-count 0)
       (frame-second-even-count 0))
  (prove:is (row-count table) element-count)
  (vellum:transform table
                    (vellum:body (first-column second-column)
                      (when (evenp first-column)
                        (incf frame-first-even-count))
                      (when (evenp second-column)
                        (incf frame-second-even-count))))
  (prove:is frame-first-even-count first-even-count)
  (prove:is frame-second-even-count second-even-count)
  (vellum:transform table
                    (vellum:body (first-column second-column)
                      (when (evenp first-column)
                        (vellum:drop-row)))
                    :in-place t)
  (setf frame-first-even-count 0)
  (vellum:transform table
                    (vellum:body (first-column second-column)
                      (when (evenp first-column)
                        (incf frame-first-even-count))
                      (when (eq :null second-column)
                        (incf null-counts))))
  (prove:is frame-first-even-count 0)
  (prove:is null-counts 0)
  (vellum:transform table
                    (vellum:body (first-column second-column)
                      (unless (= second-column (gethash first-column pairs))
                        (incf mismatch-count))))
  (prove:is mismatch-count 0))

(let* ((element-count 64325)
       (source (~> (make-array element-count)
                   (map-into (lambda ()
                               (list (random most-positive-fixnum)
                                     (random most-positive-fixnum)
                                     (random most-positive-fixnum))))))
       (indexes (~> (cl-ds.alg:zip #'list source (cl-ds:iota-range))
                    (cl-ds.alg:to-hash-table :hash-table-value #'second
                                             :hash-table-key #'first
                                             :test 'equal)))
       (pairs (cl-ds.alg:to-hash-table source
                                       :hash-table-key #'first
                                       :hash-table-value #'rest))
       (table (~> source
                  (cl-ds.alg:on-each (rcurry #'coerce 'vector))
                  (vellum:to-table :columns '((:alias first-column)
                                              (:alias second-column)
                                              (:alias third-columns)))))
       (dropped (cl-ds.alg:to-hash-table
                 (take 50300 (shuffle source))
                 :hash-table-key #'first)))
  (prove:is (vellum:row-count table) element-count)
  (vellum:transform table
                    (vellum:body (first-column)
                      (when (gethash first-column dropped)
                        (vellum:drop-row)))
                    :in-place t)
  (prove:is (vellum:row-count table) (- 64325 50300))
  (iterate
    (for i from 0 below (- 64325 50300))
    (for first-column = (vellum:at table i 0))
    (for second-column = (vellum:at table i 1))
    (for third-column = (vellum:at table i 2))
    (for index = (gethash (list first-column
                                second-column
                                third-column)
                          indexes))
    (for p-index previous index)
    (prove:isnt index nil)
    (unless (null p-index)
      (prove:ok (> index p-index)))
    (prove:is (list second-column third-column)
              (gethash first-column pairs)
              :test #'equal)))

(prove:finalize)
