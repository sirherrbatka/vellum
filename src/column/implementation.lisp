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
             :argument 'index
             :text "Column index out of column bounds."))
    (bind (((:values value found)
            (cl-ds.dicts.srrb:sparse-rrb-vector-at column index)))
      (if found value :null))))


(defmethod (setf column-at) (new-value (column sparse-material-column) index)
  (check-type index integer)
  (let ((column-size (access-column-size column)))
    (unless (< -1 index column-size)
      (error 'index-out-of-column-bounds
             :bounds `(0 ,column-size)
             :value index
             :argument 'index
             :text "Column index out of column bounds."))
    (if (eql new-value :null)
        (progn
          (cl-ds:erase! column index)
          :null)
        (progn
          (setf (cl-ds:at column index) new-value)
          new-value))))


(defmethod iterator-at ((iterator sparse-material-column-iterator)
                        column)
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
  (bind (((:slots %changes %bitmasks %columns %index %buffers) iterator)
         (buffers %buffers)
         (length (fill-pointer buffers))
         (offset (offset %index))
         (buffer (aref buffers column))
         (old-value (aref buffer offset)))
    (declare (type vector buffers))
    (setf (aref buffer offset) new-value)
    (unless (< -1 column length)
      (error 'no-such-column
             :bounds `(0 ,length)
             :value column
             :text "There is no such column."))
    (unless (eql new-value old-value)
      (setf (~> %changes (aref column) (aref offset)) t))
    new-value))


(defmethod move-iterator
    ((iterator sparse-material-column-iterator)
     times)
  (declare (optimize (debug 3)))
  (check-type times non-negative-fixnum)
  (when (zerop times)
    (return-from move-iterator nil))
  (bind (((:slots %index %stacks %buffers %depth) iterator)
         (index %index)
         (new-index (+ index times))
         (new-depth (~> new-index
                        integer-length
                        (ceiling cl-ds.common.rrb:+bit-count+)
                        1-))
         (promoted (not (eql (ceiling (1+ index)
                                      cl-ds.common.rrb:+maximum-children-count+)
                             (ceiling (1+ new-index)
                                      cl-ds.common.rrb:+maximum-children-count+)))))
    (unless promoted
      (setf %index new-index)
      (return-from move-iterator nil))
    (change-leafs iterator)
    (reduce-stacks iterator)
    (clear-changes iterator)
    (clear-buffers iterator)
    (move-stacks iterator new-index new-depth)
    (fill-buffers iterator)
    nil))


(defmethod make-iterator ((column sparse-material-column))
  (lret ((result (make 'sparse-material-column-iterator)))
    (vector-push-extend column (read-columns result))
    (vector-push-extend (make-array cl-ds.common.rrb:+maximum-children-count+)
                        (read-buffers result))
    (vector-push-extend (make-array cl-ds.common.rrb:+maximum-children-count+
                                    :initial-element nil
                                    :element-type 'boolean)
                        (read-changes result))
    (setf (access-depth result) (cl-ds.dicts.srrb:access-shift column))
    (vector-push-extend (make-array cl-ds.common.rrb:+maximal-shift+
                                    :initial-element nil)
                        (read-stacks result))
    (move-stacks result 0 (cl-ds.dicts.srrb:access-shift column))
    (setf (~> result read-stacks last-elt first-elt)
          (cl-ds.dicts.srrb:acce))
    (fill-buffers result)))


(defmethod column-type ((column sparse-material-column))
  (cl-ds:type-specialization column))


(defmethod column-type ((column fundamental-iterator))
  t)


(defmethod finish-iterator ((iterator sparse-material-column-iterator))
  (iterate
    (with depth = (access-depth iterator))
    (with index = (access-index iterator))
    (for column in-vector (read-columns iterator))
    (for stack in-vector (read-stacks iterator))
    (setf (cl-ds.dicts.srrb:access-tree column) (first-elt stack)
          (cl-ds.dicts.srrb:access-shift column) depth)
    (for index-bound = (cl-ds.dicts.srrb:scan-index-bound column))
    (setf (cl-ds.dicts.srrb:access-tree-index-bound column) index-bound
          (access-column-size column) index
          (cl-ds.dicts.srrb:access-index-bound column)
          (+ index-bound cl-ds.common.rrb:+maximum-children-count+))))
