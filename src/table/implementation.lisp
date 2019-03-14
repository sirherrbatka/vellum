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


(defmethod (setf at) (new-value
                      (frame standard-table)
                      (column symbol)
                      (row integer))
  (setf (at frame (cl-df.header:alias-to-index (header frame)
                                               column)
            row)
        new-value))


(defmethod (setf at) (new-value
                      (frame standard-table)
                      (column integer)
                      (row integer))
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


(defmethod hstack ((frame standard-table) &rest more-frames)
  (push frame more-frames)
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
          :columns new-columns)))


(defun replica-or-not (in-place)
  (check-type in-place boolean)
  (if in-place
      #'identity
      (rcurry #'cl-ds:replica t)))


(defmethod vslice ((frame standard-table) selector
                   &key in-place *transform-in-place*)
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
    (apply #'make (class-of frame)
           :header new-header
           :column new-columns
           (cl-ds.utils:cloning-information frame))))


(defmethod hslice ((frame standard-table) selector)
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (new-columns (map 'vector
                           (lambda (x)
                             (cl-df.column:make-sparse-material-column
                              :element-type (cl-df.column:column-type x)))
                           columns)))
    (if (emptyp new-columns)
        (cl-ds.utils:quasi-clone frame :columns new-columns)
        (let ((iterator (~> new-columns first-elt
                            cl-df.column:make-iterator)))
          (iterate
            (for i from 1 below column-count)
            (cl-df.column:augment-iterator iterator (aref new-columns i)))
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
          (cl-ds.utils:quasi-clone frame :columns new-columns)))))


(defmethod hmask ((frame standard-table) mask
                  &key (in-place *transform-in-place*))
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (old-size (row-count frame))
         (new-size 0)
         (new-columns (map 'vector
                           (if in-place
                               #'cl-ds:become-transactional
                               (rcurry #'cl-ds:replica t))
                           columns)))
    (if (zerop column-count)
        frame
        (cl-df.header:with-header ((header frame))
          (let ((iterator (~> new-columns first-elt
                              cl-df.column:make-iterator)))
            (iterate
              (for i from 1 below column-count)
              (cl-df.column:augment-iterator iterator
                                             (aref new-columns i)))
            (cl-df.header:set-row (make 'table-row
                                        :iterator iterator))
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
                (cl-ds.utils:quasi-clone frame
                                         :columns new-columns)))))))


(defmethod transform ((frame standard-table) function
                      &key (in-place *transform-in-place*))
  (bind ((columns (read-columns frame))
         (column-count (length columns))
         (old-size (row-count frame))
         (new-columns (map 'vector
                           (if in-place
                               #'cl-ds:become-transactional
                               (rcurry #'cl-ds:replica t))
                           columns)))
    (if (zerop column-count)
        frame
        (cl-df.header:with-header ((header frame))
          (let ((iterator (~> new-columns first-elt
                              cl-df.column:make-iterator)))
            (iterate
              (for i from 1 below column-count)
              (cl-df.column:augment-iterator iterator (aref new-columns i)))
            (cl-df.header:set-row (make 'table-row :iterator iterator))
            (iterate
              (for i from 0 below old-size)
              (funcall function)
              (cl-df.column:move-iterator iterator 1))
            (cl-df.column:finish-iterator iterator)
            (if in-place
                (progn
                  (write-columns new-columns frame)
                  frame)
                (cl-ds.utils:quasi-clone frame
                                         :columns new-columns)))))))


(defmethod cl-df.header:row-at ((header cl-df.header:standard-header)
                                (row table-row)
                                position)
  (~> row access-iterator (cl-df.column:iterator-at position)))


(defmethod (setf cl-df.header:row-at) (new-value
                                       (header cl-df.header:standard-header)
                                       (row setfable-table-row)
                                       position)
  (setf (~> row access-iterator (cl-df.column:iterator-at position))
        new-value))
