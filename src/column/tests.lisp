(in-package #:cl-df.column)

(prove:plan 512)

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
    (prove:is (column-at column i) j)))

(prove:finalize)
