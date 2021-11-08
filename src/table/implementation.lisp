(cl:in-package #:vellum.table)


(defmethod at ((frame standard-table) (row integer) column)
  (~> (column-at frame column)
      (vellum.column:column-at row)))


(defmethod (setf at) (new-value (frame standard-table)
                      (row integer) column)
  (setf (vellum.column:column-at (column-at frame column) row)
        new-value))


(defmethod column-count ((frame standard-table))
  (~> frame header vellum.header:column-count))


(defmethod row-count ((frame standard-table))
  (iterate
    (for column in-vector (read-columns frame))
    (maximize (vellum.column:column-size column))))


(defmethod column-name ((frame standard-table) (column integer))
  (~> frame header
      (vellum.header:index-to-name column)))


(defmethod column-type ((frame standard-table) column)
  (~> frame header (vellum.header:column-type column)))


(defmethod vstack* ((frame standard-table) more-frames)
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
      (vellum.column:move-iterator iterator row-count)
      (cl-ds:across
       more-frames
       (lambda (frame)
         (unless (eql column-count (column-count frame))
           (error 'vellum.header:headers-incompatible
                  :header (header frame)
                  :control-string "Inconsistent number of columns in the frames."))
         (cl-ds:traverse
          frame
          (lambda (&rest ignored)
            (declare (ignore ignored))
            (iterate
              (for i from 0 below column-count)
              (setf (vellum.column:iterator-at iterator i) (rr i)))
            (vellum.column:move-iterator iterator 1))))))
    (vellum.column:finish-iterator iterator)
    new-frame))


(defmethod hstack* ((frame standard-table) more-frames &key (isolate t))
  (cl-ds:across more-frames
                (lambda (x) (check-type x standard-table)))
  (let* ((all-frames (~>> (cl-ds.alg:to-list more-frames)
                          (cons frame)))
         (header (apply #'vellum.header:concatenate-headers
                        (mapcar #'header all-frames)))
         (column-count (vellum.header:column-count header))
         (new-columns (make-array column-count))
         (index 0))
    (declare (type fixnum index column-count)
             (type simple-vector new-columns)
             (type list more-frames))
    (iterate
      (for frame in all-frames)
      (for columns = (read-columns frame))
      (iterate
        (for column in-vector columns)
        (setf (aref new-columns index) (cl-ds:replica column isolate))
        (the fixnum (incf index))))
    (make 'standard-table
          :header header
          :columns new-columns)))


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
      (let* ((transformation (transformation frame nil
                                             :in-place in-place))
             (row (standard-transformation-row transformation)))
        (vellum.header:set-row row)
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
            (vellum.column:truncate-to-length column new-size))
          result)))))


(defmethod transformation ((frame standard-table)
                           bind-row
                           &key
                             (in-place *transform-in-place*)
                             (restarts-enabled t)
                             (start 0))
  (when (~> frame read-columns length zerop)
    (error 'cl-ds:operation-not-allowed
           :format-control "Can't transform frame without a columns."))
  (let* ((columns (cl-ds.utils:transform (lambda (x)
                                           (cl-ds.dicts.srrb:transactional-insert-tail!
                                            x
                                            (cl-ds.common.abstract:read-ownership-tag x)))
                                         (read-columns frame)))
         (bind-row-closure (bind-row-closure bind-row :header (header frame)))
         (marker-column (vellum.column:make-sparse-material-column
                         :element-type 'boolean))
         (iterator (iterator frame in-place))
         (row (make 'setfable-table-row :iterator iterator)))
    (vellum.column:move-iterator iterator start)
    (make-standard-transformation
     :marker-column marker-column
     :iterator iterator
     :bind-row-closure bind-row-closure
     :column-count (length columns)
     :restarts-enabled restarts-enabled
     :table frame
     :row row
     :in-place in-place
     :start start
     :columns columns)))


(defmethod transform-row ((object standard-transformation)
                          &optional (bind-row-closure (standard-transformation-bind-row-closure object)))
  (cl-ds.utils:with-slots-for (object standard-transformation)
    (with-table (table)
      (vellum.header:set-row row)
      (transform-row-impl object bind-row-closure))))


(defmethod transformation-result ((object standard-transformation))
  (cl-ds.utils:with-slots-for (object standard-transformation)
    (vellum.column:finish-iterator iterator)
    (let ((new-columns (vellum.column:columns iterator)))
      (assert (not (eq new-columns columns)))
      (when dropped
        (iterate
          (for i from 0 below (the fixnum (+ start count)))
          (for value = (vellum.column:column-at marker-column i))
          (if (eq :null value)
              (setf (vellum.column:column-at marker-column i) t)
              (cl-ds:erase! marker-column i)))
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


(defmethod transform ((frame standard-table)
                      bind-row
                      &key
                        (restarts-enabled t)
                        (in-place *transform-in-place*)
                        (start 0)
                        (end (row-count frame)))
  (check-type start non-negative-fixnum)
  (check-type end (or null non-negative-fixnum))
  (when (~> frame column-count zerop)
    (return-from transform
      (if in-place frame (cl-ds.utils:clone frame))))
  (with-table (frame)
    (let* ((done nil)
           (transformation (transformation frame
                                           bind-row
                                           :start start
                                           :restarts-enabled restarts-enabled
                                           :in-place in-place))
           (row (standard-transformation-row transformation))
           (*transform-control*
             (lambda (operation)
               (cond ((eq operation :finish)
                      (setf done t))
                     (t (funcall *transform-control* operation))))))
      (vellum.header:set-row row)
      (iterate
        (declare (type fixnum *current-row*))
        (for *current-row* from start)
        (until (or done
                   (and (not (null end))
                        (>= *current-row* end))))
        (transform-row-impl transformation))
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


(defmethod iterator ((frame standard-table) in-place)
  (~> frame read-columns
      (make-iterator
       :transformation (column-transformation-closure in-place))))


(defmethod cl-ds:whole-range ((container standard-table))
  (let* ((columns (read-columns container))
         (row-count (row-count container))
         (header (header container)))
    (map nil #'insert-tail columns)
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
         (row (vellum.column:index iterator))
         (header (read-header range))
         (column-count (vellum.header:column-count header)))
    (if (< row row-count)
        (iterate
          (with result = (make-array column-count))
          (for i from 0 below column-count)
          (setf (aref result i) (vellum.column:iterator-at iterator i))
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
                                  (vellum.column:index iterator)))))
    (when (zerop count)
      (return-from cl-ds:drop-front (values range count)))
    (vellum.column:move-iterator iterator count)
    (values range count)))


(defmethod cl-ds:consume-front ((range standard-table-range))
  (bind ((row-count (read-row-count range))
         (iterator (read-iterator range))
         (row (vellum.column:index iterator))
         (header (read-header range))
         (column-count (vellum.header:column-count header)))
    (if (< row row-count)
        (iterate
          (with result = (make-array column-count))
          (for i from 0 below column-count)
          (setf (aref result i) (vellum.column:iterator-at iterator i))
          (finally
           (vellum.column:move-iterator iterator 1)
           (return (values result t))))
        (values nil nil))))


(defmethod cl-ds:traverse ((range standard-table-range)
                           function)
  (ensure-functionf function)
  (bind ((iterator (read-iterator range))
         (row (read-table-row range))
         (row-count (read-row-count range)))
    (vellum.header:set-row row)
    (iterate
      (while (< (vellum.column:index iterator) row-count))
      (funcall function row)
      (vellum.column:move-iterator iterator 1))
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
      (vellum.header:set-row row)
      (iterate
        (declare (type fixnum i))
        (for i from 0 below (row-count frame))
        (funcall function row)
        (vellum.column:move-iterator iterator 1)
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


(defmethod make-table* ((class (eql 'standard-table))
                        &optional (header (vellum.header:header)))
  (check-type header vellum.header:standard-header)
  (iterate
    (with columns = (~> header
                        vellum.header:column-count
                        make-array))
    (for i from 0 below (vellum.header:column-count header))
    (setf (aref columns i)
          (vellum.column:make-sparse-material-column
           :element-type (vellum.header:column-type header i)))
    (finally (return (make 'standard-table
                           :header header
                           :columns columns)))))


(defmethod select ((frame standard-table)
                   &key
                     (rows '() rows-bound-p)
                     (columns '() columns-bound-p))
  (let ((selected-columns (if columns-bound-p
                              (with-table (frame)
                                (select-columns frame columns))
                              frame)))
    (if rows-bound-p
        (select-rows selected-columns rows)
        selected-columns)))


(defmethod alter-columns ((frame standard-table) &rest columns)
  (bind ((header (header frame))
         (column-objects (read-columns frame))
         (column-indexes
          (~> columns
              (vellum.selection:address-range
               (lambda (spec)
                 (vellum.header:ensure-index header
                                             (if (listp spec)
                                                 (first spec)
                                                 spec)))
               (column-count frame))
              cl-ds.alg:to-vector))
         (new-columns (map 'vector
                           (rcurry #'cl-ds:replica t)
                           column-objects))
         ((:values new-header old-ids)
          (vellum.header:alter-columns header column-indexes)))
    (declare (ignore old-ids))
    (cl-ds.utils:quasi-clone* frame
      :header new-header
      :columns new-columns)))


(defmethod column-at ((frame standard-table) column)
  (~>> (header frame)
       (vellum.header:name-to-index _ column)
       (column-at frame)))


(defmethod column-at ((frame standard-table) (column integer))
  (let* ((columns (read-columns frame))
         (length (array-dimension columns 0)))
    (unless (< -1 column length)
      (error 'vellum.header:no-column
             :argument 'column
             :bounds (iota length)
             :format-arguments (list column)
             :value column))
    (aref columns column)))


(defmethod erase! ((frame standard-table)
                   (row integer)
                   column)
  (~> (column-at frame column)
      (cl-ds:erase! row)))


(defmethod show ((as (eql :text))
                 (table fundamental-table)
                 &key
                   (output *standard-output*)
                   (start 0)
                   (end 10))
  (check-type table fundamental-table)
  (check-type output stream)
  (check-type start non-negative-integer)
  (check-type end non-negative-integer)
  (bind ((column-count (column-count table))
         (end (min end (row-count table)))
         (number-of-rows (max 0 (- end start)))
         (strings (make-array `(,(1+ number-of-rows) ,column-count)))
         (header (header table))
         (desired-sizes (make-array column-count
                                    :element-type 'fixnum
                                    :initial-element 0))
         ((:flet print-with-padding (row column))
          (let* ((string (aref strings row column))
                 (length (length string))
                 (desired-length (+ 2 (aref desired-sizes column))))
            (format output "~A" string)
            (unless (= (1+ column) column-count)
              (dotimes (i (- desired-length length))
                (format output "~a" #\space))))))
    (format output
            "~a columns × ~a rows. Printed rows from ~a below ~a:~%"
            column-count
            (row-count table)
            (min start end)
            end)
    (iterate
      (for j from 0 below column-count)
      (for string = (or (ignore-errors
                         (vellum.header:index-to-name header j))
                        (format nil "~a" j)))
      (setf (aref strings 0 j) string)
      (setf (aref desired-sizes j) (length string)))
    (iterate
      (for i from start below end)
      (for row from 1)
      (iterate
        (for j from 0 below column-count)
        (for string = (princ-to-string (at table i j)))
        (setf (aref strings row j) string)
        (maxf (aref desired-sizes j) (length string))))
    (iterate
      (for j from 0 below column-count)
      (print-with-padding 0 j))
    (terpri output)
    (dotimes (i (+ (reduce #'+ desired-sizes)
                   (* (1- column-count)
                      2)))
      (princ #\= output))
    (terpri output)
    (iterate
      (for i from 1 to number-of-rows)
      (iterate
        (for j from 0 below column-count)
        (print-with-padding i j))
      (terpri output))))


(defmethod print-object ((object fundamental-table) stream)
  (if *print-pretty*
      (print-unreadable-object (object stream)
        (show :text object :output stream))
      (call-next-method))
  object)


(defmethod bind-row-closure ((bind-row bind-row)
                             &key (header (header)))
  (funcall (optimized-closure bind-row)
           header))


(defmethod bind-row-closure ((bind-row (eql nil))
                             &key header)
  (declare (ignore header))
  (lambda (&rest all)
    (declare (ignore all))
    nil))


(defmethod bind-row-closure (fn
                             &key header)
  (declare (ignore header))
  (ensure-function fn))
