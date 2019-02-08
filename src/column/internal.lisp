(in-package #:cl-df.column)


(defun validate-iterator-position (iterator)
  (bind (((:slots %index %total-length) iterator))
    (unless (< %index %total-length)
      (error 'index-out-of-column-bounds
             :bounds `(0 ,%total-length)
             :value %index
             :text "Column index out of column bounds."))))


(defun offset (index)
  (declare (type fixnum index))
  (logandc2 index cl-ds.common.rrb:+tail-mask+))
