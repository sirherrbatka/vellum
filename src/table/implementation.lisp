(in-package #:cl-df.table)


(defun make-iterator (columns &key (transformation #'identity))
  (cl-df.column:make-iterator columns :transformation transformation))


(defmethod at ((frame standard-table) (column symbol) (row integer))
  (~> frame header
      (cl-df.header:alias-to-index column)
      (at frame _ row)))


(defmethod at ((frame standard-table) (column integer) (row integer))
  (check-type column non-negative-integer)
  (check-type row non-negative-integer)
  (let* ((columns (read-columns frame))
         (length (array-dimension columns 0)))
    (unless (< column length)
      (error 'cl-df.header:no-column
             :bounds (iota length)
             :format-arguments (list column)
             :value column))
    (~> (aref columns column)
        (cl-df.column:column-at row))))


(defmethod (setf at) (new-value (frame standard-table)
                      (column symbol) (row integer))
  (setf (at frame (cl-df.header:alias-to-index (header frame)
                                               column)
            row)
        new-value))


(defmethod (setf at) (new-value (frame standard-table)
                      (column integer) (row integer))
  (check-type column non-negative-integer)
  (check-type row non-negative-integer)
  (let* ((columns (read-columns frame))
         (length (array-dimension columns 0)))
    (unless (< column length)
      (error 'cl-df.header:no-column
             :bounds (iota length)
             :format-arguments (list column)
             :value column))
    (setf (cl-df.column:column-at (aref columns column) row)
          new-value)))


(defmethod column-count ((frame standard-table))
  (~> frame header cl-df.header:column-count))


(defmethod row-count ((frame standard-table))
  (~> frame read-columns
      (reduce #'max _ :key #'cl-df.column:column-size
              :initial-value 0)))


(defmethod column-name ((frame standard-table) (column integer))
  (~> frame header
      (cl-df.header:index-to-alias column)))


(defmethod column-type ((frame standard-table) column)
  (~> frame header (cl-df.header:column-type column)))


(defmethod vstack ((frame standard-table) more-frames)
  (let* ((new-columns
           (map 'vector
                (lambda (column &aux (new (cl-ds:replica column t)))
                  (~>> new
                       cl-ds.common.abstract:read-ownership-tag
                       (cl-ds.dicts.srrb:transactional-insert-tail! new))
                  new)
                (read-columns frame)))
         (iterator (make-iterator new-columns))
         (new-frame (cl-ds.utils:quasi-clone*
                        frame :columns new-columns))
         (column-count (column-count new-frame))
         (row-count (row-count new-frame)))
    (with-table (new-frame)
      (cl-df.column:move-iterator iterator row-count)
      (cl-ds:across
       more-frames
       (lambda (frame)
         (unless (eql column-count (column-count frame))
           (error 'cl-df.header:headers-incompatible
                  :header (mapcar #'header (cons frame more-frames))
                  :control-string "Inconsistent number of columns in the frames."))
         (cl-ds:traverse
          frame
          (cl-df.header:body
            (iterate
              (for i from 0 below column-count)
              (setf (cl-df.column:iterator-at iterator i)
                    (cl-df.header:rr i)))
            (cl-df.column:move-iterator iterator 1))))))
    (cl-df.column:finish-iterator iterator)
    new-frame))


(defmethod hstack ((frame standard-table) more-frames)
  (let ((more-frames (~>> (cl-ds.alg:accumulate more-frames
                                                (flip #'cons)
                                                :initial-value nil)
                          (cons frame))))
    (map nil (lambda (x) (check-type x standard-table))
         more-frames)
    (let* ((header (apply #'cl-df.header:concatenate-headers
                          (mapcar #'header more-frames)))
           (column-count (cl-df.header:column-count header))
           (new-columns (make-array column-count))
           (index 0))
      (iterate
        (for frame in more-frames)
        (for columns = (read-columns frame))
        (iterate
          (for column in-vector columns)
          (setf (aref new-columns index) (cl-ds:replica column t))
          (incf index)))
      (make 'standard-table
            :header header
            :columns new-columns))))


(defmethod vslice ((frame standard-table) selector)
  (let* ((header (header frame))
         (columns (read-columns frame))
         (column-indexes
           (cl-ds.alg:to-vector (cl-ds.alg:on-each
                                 selector
                                 (cl-ds.utils:if-else
                                  #'integerp
                                  #'identity
                                  (curry #'cl-df.header:alias-to-index
                                         header)))))
         (new-header (cl-df.header:select-columns header column-indexes))
         (new-columns (map 'vector (compose (rcurry #'cl-ds:replica t)
                                            (curry #'aref columns))
                           column-indexes)))
    (cl-ds.utils:quasi-clone* frame
      :header new-header
      :column new-columns)))


(defmethod hslice ((frame standard-table) selector)
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (new-columns (map 'vector
                           (lambda (x)
                             (cl-df.column:make-sparse-material-column
                              :element-type (cl-df.column:column-type x)))
                           columns)))
    (if (emptyp new-columns)
        (cl-ds.utils:quasi-clone* frame
          :columns new-columns)
        (let ((iterator (make-iterator columns)))
          (cl-ds:traverse
           selector
           (lambda (row)
             (iterate
               (for column in-vector new-columns)
               (for column-index from 0 below column-count)
               (setf (cl-df.column:iterator-at iterator column-index)
                     (cl-df.column:column-at column row)))
             (cl-df.column:move-iterator iterator 1)))
          (cl-df.column:finish-iterator iterator)
          (cl-ds.utils:quasi-clone* frame
            :columns new-columns)))))


(defun ensure-replicas (columns new-columns)
  (iterate
    (for i from 0 below (length new-columns))
    (for new-column = (aref new-columns i))
    (for column = (aref columns i))
    (when (eq column new-column)
      (setf (aref new-columns i)
            (cl-ds:replica new-column t))))
  new-columns)


(defmethod vmask ((frame standard-table) mask
                  &key (in-place *transform-in-place*))
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (old-size (row-count frame))
         (new-size 0))
    (if (zerop column-count)
        frame
        (cl-df.header:with-header ((header frame))
          (let* ((iterator
                   (make-iterator
                    columns
                    :transformation (rcurry #'cl-ds:replica (not in-place))))
                 (new-columns (cl-df.column:columns iterator)))
            (cl-df.header:set-row (make 'table-row :iterator iterator))
            (block out
              (cl-ds:traverse
               mask
               (lambda (accepted)
                 (unless (< new-size old-size)
                   (return-from out))
                 (when (not accepted)
                   (iterate
                     (for column in-vector new-columns)
                     (for column-index from 0 below column-count)
                     (setf (cl-df.column:iterator-at iterator column-index)
                           :null)))
                 (cl-df.column:move-iterator iterator 1)
                 (incf new-size))))
            (cl-df.column:finish-iterator iterator)
            (iterate
              (for column in-vector new-columns)
              (cl-df.column:truncate-to-length column new-size))
            (if in-place
                (progn
                  (write-columns new-columns frame)
                  frame)
                (cl-ds.utils:quasi-clone* frame
                  :columns (ensure-replicas columns new-columns))))))))


(defmethod transform ((frame standard-table) function
                      &key (in-place *transform-in-place*))
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (old-size (row-count frame)))
    (if (zerop column-count)
        frame
        (with-table (frame)
          (let* ((iterator
                   (make-iterator
                    columns
                    :transformation (rcurry #'cl-ds:replica (not in-place))))
                 (new-columns (cl-df.column:columns iterator)))
            (cl-df.header:set-row (make 'setfable-table-row
                                        :iterator iterator))
            (iterate
              (for i from 0 below old-size)
              (funcall function)
              (cl-df.column:move-iterator iterator 1))
            (cl-df.column:finish-iterator iterator)
            (if in-place
                (progn
                  (write-columns new-columns frame)
                  frame)
                (cl-ds.utils:quasi-clone* frame
                  :columns (ensure-replicas columns new-columns))))))))


(defmethod cl-df.header:row-at ((header cl-df.header:standard-header)
                                (row table-row)
                                (position symbol))
  (cl-df.header:row-at header row (cl-df.header:alias-to-index header
                                                               position)))


(defmethod (setf cl-df.header:row-at) (new-value
                                       (header cl-df.header:standard-header)
                                       (row table-row)
                                       (position symbol))
  (setf (cl-df.header:row-at header row
                             (cl-df.header:alias-to-index header
                                                          position))
        new-value))


(defmethod cl-df.header:row-at ((header cl-df.header:standard-header)
                                (row table-row)
                                position)
  (~> row read-iterator (cl-df.column:iterator-at position)))


(defmethod (setf cl-df.header:row-at) (new-value
                                       (header cl-df.header:standard-header)
                                       (row setfable-table-row)
                                       position)
  (setf (~> row read-iterator (cl-df.column:iterator-at position))
        new-value))


(defmethod cl-ds:whole-range ((container standard-table))
  (let* ((columns (read-columns container))
         (columns-count (length columns))
         (row-count (row-count container))
         (header (header container)))
    (if (zerop columns-count)
        (make 'cl-ds:empty-range)
        (make 'standard-table-range
              :table-row (make 'table-row :iterator (make-iterator columns))
              :row-count row-count
              :header header))))


(defmethod cl-ds:clone ((range standard-table-range))
  (cl-ds.utils:quasi-clone
   range
   :table-row (make 'table-row
                    :iterator (cl-ds:clone (read-iterator range)))))


(defmethod read-iterator ((range standard-table-range))
  (~> range read-table-row read-iterator))


(defmethod cl-ds:peek-front ((range standard-table-range))
  (bind ((row-count (read-row-count range))
         (iterator (read-iterator range))
         (row (cl-df.column:index iterator))
         (header (read-header range))
         (column-count (cl-df.header:column-count header)))
    (if (< row row-count)
        (iterate
          (with result = (make-array column-count))
          (for i from 0 below column-count)
          (setf (aref result i) (cl-df.column:iterator-at iterator i))
          (finally (return (values result t))))
        (values nil nil))))


(defmethod cl-ds:drop-front ((range standard-table-range)
                             count)
  (check-type count non-negative-fixnum)
  (let* ((iterator (read-iterator range))
         (count (clamp count 0 (- (read-row-count range)
                                  (cl-df.column:index iterator)))))
    (when (zerop count)
      (return-from cl-ds:drop-front (values range count)))
    (cl-df.column:move-iterator iterator count)
    (values range count)))


(defmethod cl-ds:consume-front ((range standard-table-range))
  (bind ((row-count (read-row-count range))
         (iterator (read-iterator range))
         (row (cl-df.column:index iterator))
         (header (read-header range))
         (column-count (cl-df.header:column-count header)))
    (if (< row row-count)
        (iterate
          (with result = (make-array column-count))
          (for i from 0 below column-count)
          (setf (aref result i) (cl-df.column:iterator-at iterator i))
          (finally
           (cl-df.column:move-iterator iterator 1)
           (return (values result t))))
        (values nil nil))))


(defmethod cl-ds:traverse ((range standard-table-range)
                           function)
  (ensure-functionf function)
  (bind ((iterator (read-iterator range))
         (row (read-table-row range))
         (row-count (read-row-count range)))
    (cl-df.header:set-row row)
    (iterate
      (while (< (cl-df.column:index iterator) row-count))
      (funcall function row)
      (cl-df.column:move-iterator iterator 1))
    (values nil nil)))


(defmethod cl-ds:reset! ((range standard-table-range))
  (cl-ds:reset! (read-iterator range))
  range)


(defmethod cl-ds:traverse ((table standard-table) function)
  (with-table (table)
    (cl-ds:traverse (cl-ds:whole-range table)
                    function))
  table)


(defmethod cl-ds:across ((table standard-table) function)
  (cl-ds:traverse table function))


(defmethod cl-ds.alg.meta:apply-range-function ((range standard-table)
                                                function
                                                &rest all)
  (with-table (range)
    (apply #'cl-ds.alg.meta:apply-range-function
           (cl-ds:whole-range range)
           function all)))
