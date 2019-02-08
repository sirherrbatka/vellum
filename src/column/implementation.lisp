(in-package #:cl-df.column)


(defmethod column-type ((column sparse-material-column))
  (cl-ds:type-specialization column))


(defmethod column-type ((column fundamental-iterator))
  t)


(defmethod replica ((column sparse-material-column) &optional isolate)
  (check-type isolate boolean)
  (lret ((result (make 'sparse-material-column
                       :column-size (access-column-size column)
                       :root (cl-ds.common.rrb:access-root column)
                       :shift (cl-ds.common.rrb:access-shift column)
                       :size (cl-ds.common.rrb:access-size column)
                       :tail-size (cl-ds.common.rrb:access-tail-size column)
                       :ownership-tag (cl-ds.common.abstract:make-ownership-tag)
                       :tail (and #1=(cl-ds.common.rrb:access-tail column)
                                  (copy-array #1#)))))
    (when isolate
      (cl-ds.common.abstract:write-ownership-tag
       (cl-ds.common.abstract:make-ownership-tag)
       column))))


(defmethod column-at ((column sparse-material-column) index)
  (check-type index integer)
  (let ((column-size (access-column-size column)))
    (unless (< -1 index column-size)
      (error 'index-out-of-column-bounds
             :bounds `(0 ,column-size)
             :value index
             :text "Column index out of column bounds."))
    (bind (((:values value found)
            (cl-ds:at column index)))
      (if found value :null))))


(defmethod (setf column-at) (new-value (column sparse-material-column) index)
  (check-type index integer)
  (let ((column-size (access-column-size column)))
    (unless (< -1 index column-size)
      (error 'index-out-of-column-bounds
             :bounds `(0 ,column-size)
             :value index
             :text "Column index out of column bounds."))
    (if (eql new-value :null)
        (progn
          (cl-ds:erase! column index)
          :null)
        (progn
          (setf (cl-ds:at column index) new-value)
          new-value))))


(defmethod iterator-at ((iterator sparse-material-column-iterator) column)
  (check-type column integer)
  (bind (((:slots %columns %index %buffers) iterator)
         (buffers %buffers)
         (length (fill-pointer buffers))
         (offset (offset %index)))
    (declare (type vector buffers))
    (unless (< -1 column length)
      (error 'no-such-column
             :bounds `(0 ,length)
             :value column
             :text "There is no such column."))
    (~> %buffers (aref column) (aref offset))))


(defmethod (setf iterator-at) (new-value
                               (iterator sparse-material-column-iterator)
                               column)
  (check-type column integer)
  (bind (((:slots %bitmasks %columns %index %buffers) iterator)
         (buffers %buffers)
         (length (fill-pointer buffers))
         (offset (offset %index)))
    (declare (type vector buffers))
    (setf (~> %buffers (aref column) (aref offset)) new-value)
    (unless (< -1 column length)
      (error 'no-such-column
             :bounds `(0 ,length)
             :value column
             :text "There is no such column."))
    (when (eql new-value :null)
      (let ((bitmask (aref %bitmasks column)))
        (setf (ldb (byte 1 offset) bitmask) 0
              (aref %bitmasks column) bitmask)))
    new-value))
