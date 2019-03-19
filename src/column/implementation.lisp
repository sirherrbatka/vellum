(in-package #:cl-df.column)


(defmethod column-type ((column sparse-material-column))
  (cl-ds:type-specialization column))


(defmethod column-type ((column fundamental-iterator))
  t)


(defmethod column-at ((column sparse-material-column) index)
  (sparse-material-column-at column index))


(defmethod (setf column-at) (new-value (column sparse-material-column) index)
  (setf (sparse-material-column-at column index) new-value))


(defmethod iterator-at ((iterator sparse-material-column-iterator)
                        column)
  (check-type column integer)
  (ensure-column-initialization iterator column)
  (bind (((:slots %columns %index %buffers) iterator)
         (buffers %buffers)
         (offset (offset %index)))
    (declare (type vector buffers))
    (~> buffers (aref column) (aref offset))))


(defmethod (setf iterator-at) (new-value
                               (iterator sparse-material-column-iterator)
                               column)
  (check-type column integer)
  (ensure-column-initialization iterator column)
  (bind (((:slots %changes %bitmasks %columns %index %buffers) iterator)
         (buffers %buffers)
         (offset (offset %index))
         (buffer (aref buffers column))
         (old-value (aref buffer offset)))
    (declare (type vector buffers))
    (setf (aref buffer offset) new-value)
    (unless (eql new-value old-value)
      (setf (~> (aref %changes column) (aref offset)) t))
    new-value))


(defmethod in-existing-content ((iterator sparse-material-column-iterator))
  (< (access-index iterator)
     (reduce #'max
             (read-columns iterator)
             :key #'column-size)))


(defmethod move-iterator
    ((iterator sparse-material-column-iterator)
     times)
  (declare (optimize (speed 3)))
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
         (promoted (index-promoted index new-index)))
    (declare (type fixnum index new-index new-depth)
             (type boolean promoted))
    (unless promoted
      (setf %index new-index)
      (return-from move-iterator nil))
    (let* ((initialization-status (read-initialization-status iterator))
           (depths (read-depths iterator))
           (stacks (read-stacks iterator))
           (columns (read-columns iterator))
           (changes (read-changes iterator))
           (buffers (read-buffers iterator))
           (index %index)
           (length (length depths)))
      (declare (type simple-vector stacks columns changes buffers)
               (type fixnum length)
               (type (simple-array fixnum (*)) depths)
               (type (simple-array boolean (*)) initialization-status))
      (iterate
        (declare (type fixnum i))
        (for i from 0 below length)
        (unless (aref initialization-status i)
          (next-iteration))
        (change-leaf iterator
                     (aref depths i)
                     (aref stacks i)
                     (aref columns i)
                     (aref changes i)
                     (aref buffers i))
        (reduce-stack iterator index
                      (aref depths i)
                      (aref stacks i)
                      (aref columns i))
        (map-into (the simple-vector (aref changes i))
                  (constantly nil))
        (map-into (the simple-vector (aref buffers i))
                  (constantly :null))
        (pad-stack iterator (aref depths i) index new-depth
                   (aref stacks i) (aref columns i))
        (maxf (aref depths i) new-depth)
        (move-stack (aref depths i) new-index (aref stacks i))
        (fill-buffer (aref depths i) (aref buffers i) (aref stacks i)))
      (setf %index new-index))
    nil))


(defmethod augment-iterator ((iterator (eql nil))
                             (column sparse-material-column))
  (make-iterator column))


(defun into-vector-copy (element vector)
  (lret ((result (map-into (make-array
                            (1+ (length vector))
                            :element-type (array-element-type vector))
                           #'identity
                           vector)))
    (setf (last-elt result) element)))


(defmethod augment-iterator ((iterator sparse-material-column-iterator)
                             (column sparse-material-column))
  (cl-ds.dicts.srrb:transactional-insert-tail!
   column (cl-ds.common.abstract:read-ownership-tag column))
  (let ((max-shift cl-ds.common.rrb:+maximal-shift+)
        (max-children-count cl-ds.common.rrb:+maximum-children-count+))
    (make 'sparse-material-column-iterator
          :initialization-status (into-vector-copy
                                  nil
                                  (read-initialization-status iterator))
          :columns (into-vector-copy column (read-columns iterator))
          :changes (into-vector-copy (make-array max-children-count
                                                 :element-type 'boolean
                                                 :initial-element nil)
                                     (read-changes iterator))
          :stacks (into-vector-copy (make-array max-shift
                                                :initial-element nil)
                                    (read-stacks iterator))
          :buffers (into-vector-copy (make-array max-children-count
                                                 :initial-element :null)
                                     (read-buffers iterator))
          :depths (into-vector-copy (cl-ds.dicts.srrb:access-shift column)
                                    (read-depths iterator))
          :index (access-index iterator))))


(defmethod make-iterator ((column sparse-material-column) &rest more-columns)
  (let* ((columns (coerce (cons column more-columns) 'simple-vector))
         (length (length columns))
         (max-shift cl-ds.common.rrb:+maximal-shift+)
         (max-children-count cl-ds.common.rrb:+maximum-children-count+)
         (initialization-status (make-array length :element-type 'boolean
                                                   :initial-element nil))
         (changes (map-into (make-array length)
                            (curry #'make-array max-children-count
                                   :element-type 'boolean
                                   :initial-element nil)))
         (stacks (map-into (make-array length)
                           (curry #'make-array max-shift
                                  :initial-element nil)))
         (buffers (map-into (make-array length)
                            (curry #'make-array max-children-count
                                   :initial-element :null)))
         (depths (map-into (make-array length :element-type 'fixnum)
                           #'cl-ds.dicts.srrb:access-shift columns)))
    (make 'sparse-material-column-iterator
          :initialization-status initialization-status
          :stacks stacks
          :columns columns
          :changes changes
          :buffers buffers
          :depths depths)))


(defmethod column-type ((column sparse-material-column))
  (cl-ds:type-specialization column))


(defmethod column-type ((column fundamental-iterator))
  t)


(defmethod finish-iterator ((iterator sparse-material-column-iterator))
  (change-leafs iterator)
  (reduce-stacks iterator)
  (iterate
    (for column in-vector (read-columns iterator))
    (for status in-vector (read-initialization-status iterator))
    (for column-size = (column-size column))
    (for depth in-vector (read-depths iterator))
    (setf (cl-ds.dicts.srrb:access-shift column) depth)
    (for stack in-vector (read-stacks iterator))
    (unless status
      (next-iteration))
    (setf (cl-ds.dicts.srrb:access-shift column) depth
          (cl-ds.dicts.srrb:access-tree-size column)
          (if (first-elt stack)
              (~> stack first-elt
                  (cl-ds.common.rrb:sparse-rrb-tree-size depth))
              0))
    (setf (cl-ds.dicts.srrb:access-tree column) (or (first-elt stack)
                                                    cl-ds.meta:null-bucket))
    (for index-bound = (cl-ds.dicts.srrb:scan-index-bound column))
    (setf (cl-ds.dicts.srrb:access-tree-index-bound column) index-bound
          (cl-ds.dicts.srrb:access-index-bound column)
          (* #1=cl-ds.common.rrb:+maximum-children-count+
             (1+ (ceiling index-bound #1#))))))


(defmethod cl-ds.meta:make-bucket ((operation cl-ds.meta:grow-function)
                                   (container sparse-material-column)
                                   location
                                   &rest all
                                   &key value)
  (declare (ignore all location))
  (values (cl-ds:force value)
          cl-ds.common:empty-changed-eager-modification-operation-status))


(defmethod cl-ds.meta:grow-bucket ((operation cl-ds.meta:grow-function)
                                   (container sparse-material-column)
                                   bucket
                                   location
                                   &rest all
                                   &key value)
  (declare (ignore all location bucket))
  (values (cl-ds:force value)
          cl-ds.common:empty-changed-eager-modification-operation-status))


(defmethod cl-ds.meta:position-modification
    ((operation cl-ds.meta:grow-function)
     (structure sparse-material-column)
     container
     position
     &rest all
     &key value)
  (declare (ignore all))
  (check-type position non-negative-integer)
  (when (eql value :null)
    (error 'setting-to-null
           :value value
           :format-control "Setting content of the column to :null is not allowed. Use ERASE! instead."))
  (bind (((:values result status) (call-next-method)))
    (values result status)))


(defmethod cl-ds.meta:position-modification
    ((operation cl-ds.meta:shrink-function)
     (structure sparse-material-column)
     container
     position
     &rest all)
  (declare (ignore all))
  (check-type position non-negative-integer)
  (bind (((:values result status) (call-next-method)))
    (values result status)))


(defmethod column-size ((column sparse-material-column))
  (+ (cl-ds.dicts.srrb:access-tree-index-bound column)
     (~> column cl-ds.dicts.srrb:access-tail-mask integer-length)))


(defmethod remove-nulls ((iterator sparse-material-column-iterator))
  (bind (((:slots %index %columns %stacks %buffers %depth) iterator)
         (depth (reduce #'max %columns
                        :key #'cl-ds.dicts.srrb:access-shift))
         ((:flet unify-shift (column))
          (iterate
            (for i from (cl-ds.dicts.srrb:access-shift column) below depth)
            (for node
                 initially (cl-ds.dicts.srrb:access-tree column)
                 then (make-node iterator column 1
                                 :content (vector node)))
            (finally (setf (cl-ds.dicts.srrb:access-tree column) node)))))
    (map nil #'unify-shift %columns)
    (remove-nulls-in-trees iterator)
    (concatenate-trees iterator)
    (trim-depth iterator)
    (iterate
      (for column in-vector %columns)
      (setf (cl-ds.dicts.srrb:access-tree-size column)
            (cl-ds.common.rrb:sparse-rrb-tree-size
             (cl-ds.dicts.srrb:access-tree column)
             (cl-ds.dicts.srrb:access-shift column))))
    nil))


(defmethod cl-ds:whole-range ((container sparse-material-column))
  (make-sparse-material-column-range container))


(defmethod cl-ds:across ((container sparse-material-column)
                         function)
  (~> container
      make-sparse-material-column-range
      (cl-ds:traverse function))
  container)


(defmethod cl-ds:traverse ((container sparse-material-column)
                           function)
  (cl-ds:across container function))


(defmethod truncate-to-length ((column sparse-material-column)
                               length)
  (check-type length non-negative-fixnum)
  (let ((tag (cl-ds.common.abstract:read-ownership-tag column)))
    (cl-ds.dicts.srrb:transactional-insert-tail! column tag)
    (bind ((shift (cl-ds.dicts.srrb:access-shift column))
           ((:labels impl (node byte-position))
            (let* ((i (ldb (byte cl-ds.common.rrb:+bit-count+
                                 byte-position)
                           (1- length)))
                   (mask (cl-ds.common.rrb:sparse-rrb-node-bitmask node))
                   (new-mask (ldb (byte (1+ i) 0) mask))
                   (owned (cl-ds.common.abstract:acquire-ownership node tag)))
              (when (eql mask new-mask)
                (return-from impl node))
              (unless owned
                (setf node (cl-ds.common.rrb:deep-copy-sparse-rrb-node node
                                                                       tag)))
              (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask node) new-mask)
              (unless (or (zerop byte-position)
                          (not (cl-ds.common.rrb:sparse-rrb-node-contains node
                                                                          i)))
                (setf #1=(cl-ds.common.rrb:sparse-nref node i)
                      (impl #1# (- byte-position
                                   cl-ds.common.rrb:+bit-count+))))
              node)))
      (setf #2=(cl-ds.dicts.srrb:access-tree column)
            (impl #2# (* cl-ds.common.rrb:+bit-count+ shift)))
      (trim-depth-in-column column))))


(defmethod cl-ds:clone ((iterator sparse-material-column-iterator))
  (make (class-of iterator)
         :columns (~> iterator read-columns copy-array)
         :stacks (~>> iterator read-stacks (map 'vector #'copy-array))
         :depths (~> iterator read-depths copy-array)
         :index (access-index iterator)
         :buffers (~> iterator read-buffers copy-array)
         :changes (~> iterator read-changes copy-array)
         :initialization-status (~> iterator read-initialization-status
                                    copy-array)
         ))


(defmethod cl-ds:reset! ((iterator sparse-material-column-iterator))
  (change-leafs iterator)
  (clear-changes iterator)
  (clear-buffers iterator)
  (map-into (read-initialization-status iterator) (constantly nil))
  (setf (access-index iterator) (read-initial-index iterator))
  iterator
  )
