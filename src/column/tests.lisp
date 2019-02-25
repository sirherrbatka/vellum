(in-package #:cl-df.column)

(prove:plan 1233)

(let* ((column (make-sparse-material-column))
       (iterator (make-iterator column)))
  (iterate
    (for i from 0 below 256)
    (setf (iterator-at iterator 0) i)
    (move-iterator iterator 1))
  (finish-iterator iterator)
  (iterate
    (for i from 0 below 256)
    (prove:is (column-at column i) i))
  (setf iterator (make-iterator column))
  (iterate
    (for j from 255 downto 0)
    (setf (iterator-at iterator 0) j)
    (move-iterator iterator 1))
  (finish-iterator iterator)
  (iterate
    (for j from 255 downto 0)
    (for i from 0 below 256)
    (prove:is (column-at column i) j))
  (setf iterator (make-iterator column))
  (iterate
    (for j from 128 downto 0)
    (setf (iterator-at iterator 0) j)
    (move-iterator iterator 1))
  (finish-iterator iterator)
  (iterate
    (for i from 0 below 128)
    (for j from 128 downto 0)
    (prove:is (column-at column i) j))
  (iterate
    (for i from 128 below 256)
    (for j from 127 downto 0)
    (prove:is (column-at column i) j)))

(let* ((column (make-sparse-material-column))
       (iterator (make-iterator column))
       (not-deleted (make-hash-table)))
  (iterate
    (for i from 0 below 256)
    (setf (iterator-at iterator 0) i)
    (move-iterator iterator 1))
  (finish-iterator iterator)
  (let ((nulls (~> (iota 256)
                   (coerce 'vector)
                   shuffle
                   (take 50 _))))
    (iterate
      (for i in-vector nulls)
      (erase! column i))
    (iterate
      (for i in-vector nulls)
      (prove:is (column-at column i) :null)))
  (setf iterator (make-iterator column))
  (remove-nulls iterator)
  (iterate
    (for i from 0 below (- 256 50))
    (for content = (column-at column i))
    (setf (gethash content not-deleted) t)
    (prove:isnt content :null))
  (prove:is (hash-table-count not-deleted) (- 256 50))
  (let ((vector-data (cl-ds.alg:to-vector column)))
    (prove:is (length vector-data) (- 256 50))
    (iterate
      (for i from 0 below (- 256 50))
      (prove:is (aref vector-data i) (column-at column i))))
  (prove:is (column-size column) (- 256 50)))

(prove:finalize)
