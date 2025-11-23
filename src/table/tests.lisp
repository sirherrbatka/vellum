(cl:in-package #:vellum.table)

(prove:plan 8220)

(let* ((data-frame (vellum:to-table (cl-ds.alg:on-each (cl-ds:iota-range :to 308) #'list)
                                    :columns '(column)
                                    :wrap-errors nil
                                    :enable-restarts nil))
       (transformed (vellum:transform data-frame
                                      (vellum:bind-row (column)
                                        (unless (= column 108)
                                          (vellum:drop-row)))
                      :wrap-errors nil
                      :enable-restarts nil)))
  (prove:is (vellum:row-count transformed) 1)
  (prove:is (vellum:at transformed 0 0) 108))

(defparameter *test-data* #(#(1 a 5 s)
                              #(2 b 6 s)
                              #(3 c 7 s)))

(vellum:with-standard-header (nil nil nil nil)
    (defparameter *table*
      (~> *test-data*
          cl-ds:whole-range
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
    (vellum:bind-row ()
      (setf (vellum:rr 0) (+ 1 (vellum:rr 0))))
    :in-place nil))

(prove:is (vellum:at *table* 0 0) 1)
(prove:is (vellum:at *table* 1 0) 2)
(prove:is (vellum:at *table* 2 0) 3)

(prove:is (vellum:at *replica* 0 0) 2)
(prove:is (vellum:at *replica* 1 0) 3)

(prove:is (vellum:at *replica* 2 0) 4)

(vellum:transform *table*
  (vellum:bind-row ()
    (setf (vellum:rr 0) (* 2 (vellum:rr 0))))
  :in-place t)

(prove:is (vellum:at *table* 0 0) 2)
(prove:is (vellum:at *table* 1 0) 4)
(prove:is (vellum:at *table* 2 0) 6)

(prove:is (vellum:at *replica* 0 0) 2)
(prove:is (vellum:at *replica* 1 0) 3)
(prove:is (vellum:at *replica* 2 0) 4)

(defparameter *concatenated-table* (vellum:vstack (list *table* *replica*)))

(prove:is (column-count *concatenated-table*) 4)
(prove:is (row-count *concatenated-table*) 6)

(prove:is (vellum:at *concatenated-table* 0 0) 2)
(prove:is (vellum:at *concatenated-table* 1 0) 4)
(prove:is (vellum:at *concatenated-table* 2 0) 6)
(prove:is (vellum:at *concatenated-table* 3 0) 2)
(prove:is (vellum:at *concatenated-table* 4 0) 3)
(prove:is (vellum:at *concatenated-table* 5 0) 4)

(defparameter *sub-table* (select *concatenated-table*
                            :rows (vellum:s (vellum:between :from 1 :to 4))))

(prove:is (column-count *sub-table*) 4)
(prove:is (row-count *sub-table*) 3)
(prove:is (at *sub-table* 0 0) 4)
(prove:is (at *sub-table* 1 0) 6)
(prove:is (at *sub-table* 2 0) 2)

(defparameter *sub-table* (select *concatenated-table*
                            :columns (vellum:s (vellum:between :to 3))))

(prove:is (column-count *sub-table*) 3)
(prove:is (row-count *sub-table*) 6)

(defparameter *sub-table* (select *concatenated-table*
                            :rows '(1 2 3)))
(prove:is (column-count *sub-table*) 4)
(prove:is (row-count *sub-table*) 3)
(prove:is (at *sub-table* 0 0) 4)
(prove:is (at *sub-table* 1 0) 6)
(prove:is (at *sub-table* 2 0) 2)

(let* ((element-count 2525)
       (source (~> (make-list element-count)
                   (map-into (lambda ()
                               (vector (random most-positive-fixnum)
                                       (random most-positive-fixnum))))))
       (pairs (cl-ds.alg:to-hash-table source
                                       :hash-table-key #'first-elt
                                       :hash-table-value (rcurry #'aref 1)))
       (table (vellum:to-table source
                               :columns '((:name first-column)
                                          (:name second-column))))
       (first-even-count (count-if #'evenp source :key #'first-elt))
       (second-even-count (count-if #'evenp source :key (rcurry #'aref 1)))
       (frame-first-even-count 0)
       (null-counts 0)
       (mismatch-count 0)
       (frame-second-even-count 0))
  (prove:is (row-count table) element-count)
  (vellum:transform table
                    (vellum:bind-row (first-column second-column)
                      (when (evenp first-column)
                        (incf frame-first-even-count))
                      (when (evenp second-column)
                        (incf frame-second-even-count))))
  (prove:is frame-first-even-count first-even-count)
  (prove:is frame-second-even-count second-even-count)
  (vellum:transform table
                    (vellum:bind-row (first-column second-column)
                      (when (evenp first-column)
                        (vellum:drop-row)))
                    :in-place t)
  (setf frame-first-even-count 0)
  (vellum:transform table
                    (vellum:bind-row (first-column second-column)
                      (when (evenp first-column)
                        (incf frame-first-even-count))
                      (when (eq :null second-column)
                        (incf null-counts))))
  (prove:is frame-first-even-count 0)
  (prove:is null-counts 0)
  (vellum:transform table
                    (vellum:bind-row (first-column second-column)
                      (unless (= second-column (gethash first-column pairs))
                        (incf mismatch-count))))
  (prove:is mismatch-count 0))

(let* ((element-count 6432)
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
                  (vellum:to-table :columns '((:name first-column)
                                              (:name second-column)
                                              (:name third-column)))))
       (renamed (vellum:select table :columns '((first-column renamed-first-column)
                                                (second-column renamed-second-column)
                                                (third-column renamed-third-column))))
       (dropped (cl-ds.alg:to-hash-table
                 (take 5030 (shuffle source))
                 :hash-table-key #'first)))
  (prove:is (vellum:row-count table) element-count)
  (prove:is (vellum:row-count renamed) element-count)
  (prove:is (~> renamed vellum.table:header (vellum.header:column-signature 0)
                vellum.header:read-name)
            "RENAMED-FIRST-COLUMN"
            :test #'string=)
  (prove:is (~> renamed vellum.table:header (vellum.header:column-signature 1)
                vellum.header:read-name)
            "RENAMED-SECOND-COLUMN"
            :test #'string=)
  (prove:is (~> renamed vellum.table:header (vellum.header:column-signature 2)
                vellum.header:read-name)
            "RENAMED-THIRD-COLUMN"
            :test #'string=)
  (vellum:transform table
                    (vellum:bind-row (first-column)
                      (when (gethash first-column dropped)
                        (vellum:drop-row)))
                    :in-place t)
  (prove:is (vellum:row-count table) (- 6432 5030))
  (iterate
    (for i from 0 below (- 6432 5030))
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

(let* ((frame (to-table (mapcar #'list (iota 32))
                        :columns '(number))))
  (prove:is (row-count frame) 32))

(let* ((frame (to-table (mapcar #'list (iota 32))
                        :columns '(number)
                        :body (vellum:bind-row (number)
                                (when (evenp number)
                                  (drop-row))))))
  (prove:is (row-count frame) 16))

(let* ((frame (to-table (mapcar #'list (iota 63))
                        :columns '(number)
                        :body (vellum:bind-row (number)
                                (when (evenp number)
                                  (drop-row))))))
  (prove:is (row-count frame) 31))

(let* ((frame (to-table (mapcar #'list (iota 63))
                        :columns '(number)
                        :body (vellum:bind-row (number)
                                (when (oddp number)
                                  (drop-row))))))
  (prove:is (row-count frame) 32))

(let* ((frame (to-table (mapcar #'list (iota 32))
                        :columns '(number)
                        :body (vellum:bind-row (number)
                                (when (oddp number)
                                  (drop-row))))))
  (prove:is (row-count frame) 16))

(let* ((frame (to-table (mapcar #'list (iota 62))
                        :columns '(number)
                        :body (vellum:bind-row (number)
                                (unless (zerop (truncate number 3))
                                  (drop-row))))))
  (prove:is (row-count frame) 3))

(let* ((frame (to-table (mapcar #'list (iota 62))
                        :columns '(number)
                        :body (vellum:bind-row (number)
                                (unless (zerop (mod number 5))
                                  (drop-row))))))
  (prove:is (row-count frame) 13))

(let* ((frame (transform
                  (make-table :columns '(number))
                (vellum:bind-row (number)
                  (setf number *current-row*)
                  (unless (zerop (mod number 62))
                    (drop-row)))
                :end (* 32 62))))
  (prove:is (row-count frame) 32))

(let* ((frame (transform
                  (make-table :columns '(number))
                (vellum:bind-row (number)
                  (setf number *current-row*)
                  (unless (zerop (mod number 5))
                    (drop-row)))
                :end 62)))
  (prove:is (row-count frame) 13))

(handler-bind ((error (lambda (c)
                        (declare (ignore c))
                        (invoke-restart 'drop-row))))
  (let ((frame (transform
                (make-table :columns '(number))
                (vellum:bind-row (number)
                  (setf number *current-row*)
                  (assert (zerop (mod number 5))))
                :end 62)))
    (prove:is (row-count frame) 13)))

(handler-bind ((error (lambda (c)
                        (declare (ignore c))
                        (invoke-restart 'retry))))
  (let* ((index 0)
         (calls 0)
         (frame (transform
                 (make-table :columns '(number))
                 (vellum:bind-row (number)
                   (incf calls)
                   (setf number (incf index))
                   (assert (zerop (mod number 5))))
                 :end 62)))
    (prove:is (row-count frame) 62)
    (prove:is calls 310)
    (prove:ok (every (compose #'zerop (rcurry #'mod 5))
                     (vellum:pipeline (frame)
                       (cl-ds.alg:to-list :key (vellum:brr number)))))))

(let* ((index 0)
       (frame (handler-bind
                  ((error (lambda (c)
                            (declare (ignore c))
                            (invoke-restart 'vellum.header:skip-row))))
                (transform
                 (make-table :columns '(number))
                 (vellum:bind-row (number)
                   (setf number (incf index))
                   (assert (zerop (mod number 5))))
                 :end 62))))
  (prove:is (row-count frame) 60)
  (prove:ok (every (lambda (x)
                     (or (eq :null x)
                         (zerop (mod x 5))))
                   (vellum:pipeline (frame)
                     (cl-ds.alg:to-list :key (vellum:brr number))))))

(let ((table (~> (iota 1500)
                 (cl-ds.alg:on-each #'list)
                 (vellum:to-table :columns '(number))
                 (vellum:add-columns 'column))))
  (iterate
    (for i from 0 below 1032)
    (setf (at table i 'column) i))
  (iterate
    (for i from 1032 below 1500)
    (setf (at table i 'column) t))
  (iterate
    (for i from 1032 below 1500)
    (prove:is (at table i 'column) t))
  (iterate
    (for i from 0 below 1032)
    (prove:is (at table i 'column) i))
  (vellum:transform table
                    (vellum:bind-row (column)
                      (when (integerp column)
                        (vellum:drop-row)))
                    :in-place t)
  (prove:is (vellum:row-count table)
            (- 1500 1032))
  (iterate
    (for i from 1032 below 1500)
    (for j from 0)
    (prove:is (at table j 'column) t)
    (prove:is (at table j 'number) i)))

(let ((table (make-table :columns '(test1 test2))))
  (iterate
    (for i from 0 below 1300)
    (setf (vellum:at table i 'test1) i))
  (iterate
    (for i from 0 below 500)
    (setf (vellum:at table i 'test2) i))
  (prove:is (vellum:row-count table) 1300)
  (vellum:transform table
                    (vellum:bind-row (test1 test2)
                      (prove:is test2 (vellum:at table *current-row* 'test2)))
    :in-place t)
  (prove:is (vellum:row-count table) 1300)
  (vellum:transform table
    (vellum:bind-row () (vellum:drop-row))
    :in-place t
    :end (vellum:row-count table))
  (prove:is (vellum:row-count table) 0))

(let* ((iota (iota 80000))
       (table (vellum:make-table :columns '(i)))
       (transformation (vellum.table:transformation table
                                                    (vellum:bind-row (i)
                                                      (setf i (pop iota)))
                                                    :in-place t)))
  (iterate
    (repeat 80000)
    (vellum:transform-row transformation))
  (vellum.table:transformation-result transformation)
  (prove:is (vellum:pipeline (table) (cl-ds.alg:to-list :key (vellum:brr i))) (iota 80000)))

(let* ((iota (iota 80000))
       (table (vellum:to-table (mapcar #'list iota)
                               :columns '(i))))
  (prove:is (vellum:pipeline (table) (cl-ds.alg:to-list :key (vellum:brr i))) iota))

(iterate
  (with table = (vellum:to-table '()
                                 :columns '((:name a)
                                            (:name b)
                                            (:name c))))
  (for i from 0 below 100)
  (prove:is (vellum:row-count table) i)
  (iterate
    (for ii from 0 below 3)
    (setf (vellum:at table i ii)
          5)))

(iterate
  (with table = (vellum:to-table '()
                                 :columns '((:name a)
                                            (:name b)
                                            (:name c))))
  (for i from 0 below 100)
  (for current-row = (vellum:row-count table))
  (prove:is current-row i)
  (iterate
    (for ii from 0 below 3)
    (setf (vellum:at table current-row ii)
          5)))

(prove:finalize)
