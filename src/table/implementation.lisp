(in-package #:cl-df.table)


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
  (or (iterate
        (for column in-vector (read-columns frame))
        (maximize (cl-df.column:column-size column)))
      0))


(defmethod column-name ((frame standard-table) (column integer))
  (~> frame header
      (cl-df.header:index-to-alias column)))


(defmethod column-type ((frame standard-table) column)
  (~> frame header (cl-df.header:column-type column)))


(defmethod hstack ((frame standard-table) more-frames)
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
                  :header (header frame)
                  :control-string "Inconsistent number of columns in the frames."))
         (cl-ds:traverse
          frame
          (cl-df.header:body ()
            (iterate
              (for i from 0 below column-count)
              (setf (cl-df.column:iterator-at iterator i)
                    (cl-df.header:rr i)))
            (cl-df.column:move-iterator iterator 1))))))
    (cl-df.column:finish-iterator iterator)
    new-frame))


(defmethod vstack ((frame standard-table) more-frames)
  (cl-ds:across more-frames
                (lambda (x) (check-type x standard-table)))
  (let* ((more-frames (~>> (cl-ds.alg:accumulate more-frames
                                                 (flip #'cons)
                                                 :initial-value nil)
                           nreverse
                           (cons frame)))
         (header (apply #'cl-df.header:concatenate-headers
                        (mapcar #'header more-frames)))
         (column-count (cl-df.header:column-count header))
         (new-columns (make-array column-count))
         (index 0))
    (declare (type fixnum index column-count)
             (type simple-vector new-columns)
             (type list more-frames))
    (iterate
      (for frame in more-frames)
      (for columns = (read-columns frame))
      (iterate
        (for column in-vector columns)
        (setf (aref new-columns index) (cl-ds:replica column t))
        (the fixnum (incf index))))
    (make 'standard-table
          :header header
          :columns new-columns)))


(defmethod vselect ((frame standard-table) selector)
  (let* ((header (header frame))
         (columns (read-columns frame))
         (column-indexes (~>> (curry #'cl-df.header:alias-to-index header)
                              (cl-ds.utils:if-else #'integerp #'identity)
                              (cl-ds.alg:on-each selector)
                              cl-ds.alg:to-vector))
         (new-header (cl-df.header:select-columns header column-indexes))
         (new-columns (map 'vector (compose (rcurry #'cl-ds:replica t)
                                            (curry #'aref columns))
                           column-indexes)))
    (declare (type simple-vector columns new-columns))
    (cl-ds.utils:quasi-clone* frame
      :header new-header
      :columns new-columns)))


(defmethod hselect ((frame standard-table) (selector selection))
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (starts (read-starts selector))
         (ends (read-ends selector))
         (new-columns (map 'vector
                           (lambda (x)
                             (cl-df.column:make-sparse-material-column
                              :element-type (cl-df.column:column-type x)))
                           columns)))
    (declare (type simple-vector new-columns columns)
             (type fixnum column-count))
    (when (emptyp new-columns)
      (return-from hselect (cl-ds.utils:quasi-clone* frame
                            :columns new-columns)))
    (iterate
      (with iterator = (make-iterator new-columns))
      (for start in-vector starts)
      (for end in-vector ends)
      (for source-iterator = (iterator frame t))
      (cl-df.column:move-iterator source-iterator start)
      (iterate
        (for i
             from start
             below end)
        (iterate
          (declare (type fixnum column-index))
          (for column-index from 0 below column-count)
          (for column = (aref new-columns column-index))
          (setf (cl-df.column:iterator-at iterator column-index)
                (cl-df.column:iterator-at source-iterator column-index)))
        (cl-df.column:move-iterator iterator 1)
        (cl-df.column:move-iterator source-iterator 1))
      (finally (cl-df.column:finish-iterator iterator)))
    (cl-ds.utils:quasi-clone* frame
      :columns new-columns)))


(defmethod hselect ((frame standard-table) selector)
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (new-columns (map 'vector
                           (lambda (x)
                             (cl-df.column:make-sparse-material-column
                              :element-type (cl-df.column:column-type x)))
                           columns)))
    (declare (type simple-vector new-columns)
             (type fixnum column-count))
    (when (emptyp new-columns)
      (return-from hselect (cl-ds.utils:quasi-clone* frame
                            :columns new-columns)))
    (let ((iterator (make-iterator new-columns)))
      (cl-ds:traverse
       selector
       (lambda (row)
         (iterate
           (declare (type fixnum column-index))
           (for column-index from 0 below column-count)
           (for column = (aref columns column-index))
           (setf (cl-df.column:iterator-at iterator column-index)
                 (cl-df.column:column-at column row)))
         (cl-df.column:move-iterator iterator 1)))
      (cl-df.column:finish-iterator iterator)
      (cl-ds.utils:quasi-clone* frame
        :columns new-columns))))


(defmethod vmask ((frame standard-table) mask
                  &key (in-place *transform-in-place*))
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (old-size (row-count frame))
         (new-size 0))
    (declare (type fixnum new-size old-size column-count))
    (when (zerop column-count)
      (return-from vmask
        (if in-place frame (cl-ds.utils:quasi-clone* frame))))
    (with-table (frame)
      (let* ((transformation (transformation frame :in-place in-place))
             (row (standard-transformation-row transformation)))
        (cl-df.header:set-row row)
        (block out
          (cl-ds:traverse
           mask
           (lambda (accepted)
             (unless (< new-size old-size)
               (return-from out))
             (transform-row-impl transformation
                                 (lambda (&rest all)
                                   (declare (ignore all))
                                   (unless accepted
                                     (nullify))))
             (incf new-size))))
        (let* ((result (transformation-result transformation))
               (new-columns (read-columns result)))
          (iterate
            (for column in-vector new-columns)
            (cl-df.column:truncate-to-length column new-size))
          result)))))


(defmethod transformation ((frame standard-table)
                           &key
                             (in-place *transform-in-place*)
                             (start 0))
  (when (~> frame read-columns length zerop)
    (error 'cl-ds:operation-not-allowed
           :format-control "Can't transform frame without a columns."))
  (let* ((columns (read-columns frame))
         (marker-column (cl-df.column:make-sparse-material-column
                         :element-type 'boolean))
         (marker-iterator (make-iterator (vector marker-column)))
         (iterator (iterator frame in-place))
         (row (make 'setfable-table-row :iterator iterator)))
    (cl-df.column:move-iterator iterator start)
    (cl-df.column:move-iterator marker-iterator start)
    (make-standard-transformation
     :marker-iterator marker-iterator
     :marker-column marker-column
     :iterator iterator
     :column-count (length columns)
     :table frame
     :row row
     :in-place in-place
     :start start
     :columns columns)))


(defmethod transform-row ((object standard-transformation)
                          function)
  (ensure-functionf function)
  (cl-ds.utils:with-slots-for (object standard-transformation)
    (with-table (table)
      (cl-df.header:set-row row)
      (transform-row-impl object function))))


(defmethod transformation-result ((object standard-transformation))
  (cl-ds.utils:with-slots-for (object standard-transformation)
    (cl-df.column:finish-iterator iterator)
    (let ((new-columns (cl-df.column:columns iterator))
          (marker-iterator marker-iterator))
      (assert (not (eq new-columns columns)))
      (when dropped
        (cl-df.column:finish-iterator marker-iterator)
        (setf marker-iterator (make-iterator (vector marker-column)))
        (iterate
          (for i from 0 below (the fixnum (+ start count)))
          (for value = (cl-df.column:iterator-at marker-iterator 0))
          (setf (cl-df.column:iterator-at marker-iterator 0)
                (if (eql :null value) t :null))
          (cl-df.column:move-iterator marker-iterator 1))
        (cl-df.column:finish-iterator marker-iterator)
        (let ((cleaned-columns (adjust-array new-columns
                                             (1+ column-count))))
          (setf (last-elt cleaned-columns) marker-column
                new-columns (~> cleaned-columns
                                remove-nulls-from-columns
                                (adjust-array column-count)))))
      (if in-place
          (progn
            (write-columns new-columns table)
            table)
          (cl-ds.utils:quasi-clone* table
            :columns (ensure-replicas columns new-columns))))))


(defmethod transform ((frame standard-table) function
                      &key
                        (in-place *transform-in-place*)
                        (start 0)
                        (end (row-count frame)))
  (declare (optimize (speed 3)))
  (ensure-functionf function)
  (check-type start non-negative-fixnum)
  (check-type end (or null non-negative-fixnum))
  (when (~> frame column-count zerop)
    (return-from transform
      (if in-place frame (cl-ds.utils:quasi-clone* frame))))
  (with-table (frame)
    (let* ((done nil)
           (transformation (transformation frame :start start
                                                 :in-place in-place))
           (row (standard-transformation-row transformation))
           (*transform-control*
             (lambda (operation)
               (cond ((eq operation :finish) (setf done t))
                     (t (funcall *transform-control* operation))))))
      (cl-df.header:set-row row)
      (iterate
        (declare (type fixnum i))
        (for i from start)
        (until (or done
                   (and (not (null end))
                        (>= i end))))
        (transform-row-impl transformation function))
      (transformation-result transformation))))


(defmethod remove-nulls ((frame standard-table)
                         &key (in-place *transform-in-place*))
  (let* ((columns (read-columns frame))
         (new-columns (remove-nulls-from-columns columns
                       (curry #'cl-ds:replica (not in-place)))))
    (when (eq columns new-columns)
      (return-from remove-nulls
        (if in-place
            frame
            (cl-ds.utils:quasi-clone* frame
              :columns (map 'vector
                            (lambda (x) (cl-ds:replica x t))
                            columns)))))
    (if in-place
        (progn
          (write-columns new-columns frame)
          frame)
        (cl-ds.utils:quasi-clone* frame
          :columns (ensure-replicas columns new-columns)))))


(defmethod cl-df.header:row-at ((header cl-df.header:standard-header)
                                (row table-row)
                                (position string))
  (cl-df.header:row-at header row (cl-df.header:alias-to-index header
                                                               position)))


(defmethod cl-df.header:row-at ((header cl-df.header:standard-header)
                                (row table-row)
                                (position symbol))
  (cl-df.header:row-at header row (cl-df.header:alias-to-index header
                                                               position)))


(defmethod (setf cl-df.header:row-at) (new-value
                                       (header cl-df.header:standard-header)
                                       (row setfable-table-row)
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


(defmethod iterator ((frame standard-table) in-place)
  (~> frame read-columns
      (make-iterator
       :transformation (column-transformation-closure in-place))))


(defmethod cl-ds:whole-range ((container standard-table))
  (let* ((columns (read-columns container))
         (row-count (row-count container))
         (header (header container)))
    (if (~> columns length zerop)
        (make 'cl-ds:empty-range)
        (make 'standard-table-range
              :table-row (make 'table-row :iterator (iterator container t))
              :row-count row-count
              :header header))))


(defmethod cl-ds:clone ((range standard-table-range))
  (cl-ds.utils:quasi-clone* range
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


(defmethod cl-ds:become-transactional ((container standard-table))
  (cl-ds:replica container))


(defmethod cl-ds:replica ((container standard-table) &optional isolate)
  (cl-ds.utils:quasi-clone* container
    :columns (~>> container read-columns
                  (map 'vector (rcurry #'cl-ds:replica isolate)))))


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


(defmethod cl-ds:traverse ((frame standard-table) function)
  (when (~> frame column-count zerop)
    (return-from cl-ds:traverse frame))
  (with-table (frame)
    (let* ((iterator (iterator frame t))
           (row (make-instance 'table-row :iterator iterator)))
      (cl-df.header:set-row row)
      (iterate
        (declare (type fixnum i))
        (for i from 0 below (row-count frame))
        (funcall function row)
        (cl-df.column:move-iterator iterator 1)
        (finally (return frame))))))


(defmethod cl-ds:across ((table standard-table) function)
  (cl-ds:traverse table function))


(defmethod cl-ds.alg.meta:apply-range-function ((range fundamental-table)
                                                (function cl-ds.alg.meta:layer-function)
                                                all)
  (cl-ds.alg.meta:apply-layer (cl-ds:whole-range range) function all))


(defmethod cl-ds.alg.meta:apply-range-function ((range fundamental-table)
                                                (function cl-ds.alg.meta:aggregation-function)
                                                all)
  (cl-ds.alg.meta:apply-aggregation-function range function all))


(defmethod cl-ds:traverse ((selection selection)
                           function)
  (ensure-functionf function)
  (iterate
    (for i from (read-start selection) below (read-end selection))
    (funcall function i))
  selection)


(defmethod cl-ds:across ((selection selection)
                         function)
  (cl-ds:traverse selection function))


(defmethod make-table ((class (eql 'standard-table))
                       &optional (header (cl-df.header:header)))
  (check-type header cl-df.header:standard-header)
  (make 'standard-table
        :header header
        :columns (iterate
                   (with columns = (~> header
                                       cl-df.header:column-count
                                       make-array))
                   (for i from 0 below (cl-df.header:column-count header))
                   (setf (aref columns i)
                         (cl-df.column:make-sparse-material-column
                          :element-type (cl-df.header:column-type header i)))
                   (finally (return columns)))))
