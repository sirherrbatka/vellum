(cl:in-package #:vellum.column)


(defmethod column-type ((column sparse-material-column))
  (cl-ds:type-specialization column))


(defmethod column-at ((column sparse-material-column) index)
  (sparse-material-column-at column index))


(defmethod (setf column-at) (new-value (column sparse-material-column) index)
  (setf (sparse-material-column-at column index) new-value))


(-> iterator-at (sparse-material-column-iterator integer) t)
(declaim (inline iterator-at))
(defun iterator-at (iterator column)
  (ensure-column-initialization iterator column)
  (bind ((buffers (read-buffers iterator))
         (offset (offset (index iterator))))
    (declare (type simple-vector buffers))
    (~> (the iterator-buffer (aref buffers column)) (aref offset))))


(defun (setf iterator-at) (new-value iterator column)
  (check-type column integer)
  (check-type iterator sparse-material-column-iterator)
  (ensure-column-initialization iterator column)
  (bind ((buffers (read-buffers iterator))
         (index (the fixnum (index iterator)))
         (offset (the fixnum (offset index)))
         (buffer (aref buffers column))
         (expected-type (~> iterator
                            sparse-material-column-iterator-columns
                            (aref column)
                            cl-ds:type-specialization))
         (old-value (aref buffer offset)))
    (declare (type simple-vector buffers))
    (unless (or (eq :null new-value)
                (typep new-value expected-type))
      (error 'column-type-error
             :expected-type expected-type
             :column column
             :datum new-value))
    (setf (aref buffer offset) new-value)
    (unless (eql new-value old-value)
      (let ((columns (read-columns iterator))
            (transformation (ensure-function (read-transformation iterator)))
            (changes (read-changes iterator))
            (touched (read-touched iterator)))
        (declare (type simple-vector columns changes)
                 (type (simple-array boolean (*)) touched))
        (unless (aref touched column)
          (setf #1=(aref columns column) (funcall transformation #1#)))
        (setf (~> (aref changes column) (aref offset)) t
              (aref touched column) t)))
    new-value))


(defun untouch (iterator)
  (iterate
    (with depths = (read-depths iterator))
    (with offset = (offset (index iterator)))
    (with changes = (read-changes iterator))
    (with buffers = (read-buffers iterator))
    (with stacks = (read-stacks iterator))
    (for change in-vector changes)
    (for buffer in-vector buffers)
    (for depth in-vector depths)
    (for stack in-vector stacks)
    (for node = (aref stack depth))
    (setf (aref change offset) nil
          (aref buffer offset) (cond ((null node)
                                      :null)
                                     ((cl-ds.common.rrb:sparse-rrb-node-contains node offset)
                                      (cl-ds.common.rrb:sparse-nref node offset))
                                     (t :null)))))


(defmethod in-existing-content ((iterator sparse-material-column-iterator))
  (< (access-index iterator)
     (reduce #'max
             (read-columns iterator)
             :initial-value 0
             :key #'column-size)))


(-> move-iterator-to (sparse-material-column-iterator non-negative-fixnum) t)
(defun move-iterator-to (iterator new-index)
  (bind ((new-depth (calculate-depth new-index))
         (depths (read-depths iterator))
         (length (length depths)))
    (declare (type fixnum length)
             (type (simple-array fixnum (*)) depths))
    (iterate
      (declare (type fixnum i))
      (for i from 0 below length)
      (move-column-to iterator new-index i :depth new-depth))
    (setf (access-index iterator) new-index)
    nil))


(-> move-iterator (sparse-material-column-iterator non-negative-fixnum) t)
(defun move-iterator (iterator times)
  (move-iterator-to iterator (+ (sparse-material-column-iterator-index iterator)
                                times)))

(defun into-vector-copy (element vector)
  (lret ((result (map-into (make-array
                            (1+ (length vector))
                            :element-type (array-element-type vector))
                           #'identity
                           vector)))
    (setf (last-elt result) element)))


(defmethod augment-iterator ((iterator sparse-material-column-iterator)
                             (column sparse-material-column))
  (~>> column
       cl-ds.common.abstract:read-ownership-tag
       (cl-ds.dicts.srrb:transactional-insert-tail! column))
  (let* ((max-shift cl-ds.common.rrb:+maximal-shift+)
         (shift (cl-ds.dicts.srrb:access-shift column))
         (max-children-count cl-ds.common.rrb:+maximum-children-count+)
         (stack (make-array max-shift :initial-element nil))
         (buffers (make-array max-children-count
                              :initial-element :null))
         (changes (make-array max-children-count
                              :element-type 'boolean
                              :initial-element nil)))
    (make-sparse-material-column-iterator
     :indexes (~>> iterator read-indexes
                   (into-vector-copy -1))
     :index (index iterator)
     :initial-index (index iterator)
     :touched (~>> iterator read-touched
                   (into-vector-copy nil))
     :initialization-status (~>> iterator
                                 read-initialization-status
                                 (into-vector-copy nil))
     :columns (~>> iterator read-columns
                   (into-vector-copy column))
     :changes (~>> iterator read-changes
                   (into-vector-copy changes))
     :stacks (~>> iterator read-stacks
                  (into-vector-copy stack))
     :buffers (~>> iterator read-buffers
                   (into-vector-copy buffers))
     :depths (~>> iterator read-depths
                  (into-vector-copy shift)))))


(defmethod make-iterator (columns &key (transformation #'identity))
  (let* ((columns (~> columns
                      cl-ds.alg:to-vector
                      cl-ds.utils:remove-fill-pointer))
         (length (length columns))
         (max-shift cl-ds.common.rrb:+maximal-shift+)
         (max-children-count cl-ds.common.rrb:+maximum-children-count+)
         (initialization-status (make-array length :element-type 'boolean
                                                   :initial-element nil))
         (changes (map-into (make-array length)
                            (curry #'make-array max-children-count
                                   :element-type 'boolean
                                   :initial-element nil)))
         (touched (make-array length :element-type 'boolean
                                     :initial-element nil))
         (stacks (map-into (make-array length)
                           (curry #'make-array max-shift
                                  :initial-element nil)))
         (buffers (map-into (make-array length)
                            (curry #'make-array max-children-count
                                   :initial-element :null)))
         (depths (map-into (make-array length :element-type 'fixnum)
                           #'cl-ds.dicts.srrb:access-shift columns)))
    (make-sparse-material-column-iterator
     :initialization-status initialization-status
     :indexes (make-array length :element-type 'fixnum :initial-element -1)
     :stacks stacks
     :columns columns
     :changes changes
     :buffers buffers
     :touched touched
     :transformation transformation
     :depths depths)))


(defmethod finish-iterator ((iterator sparse-material-column-iterator))
  (change-leafs iterator)
  ;; this prohibits reducing stacks directly after they have been reduced by move-iterator
  (iterate
    (for column in-vector (read-columns iterator))
    (for touched in-vector (read-touched iterator))
    (for depth in-vector (read-depths iterator))
    (for index in-vector (read-indexes iterator))
    (for stack in-vector (read-stacks iterator))
    (unless touched
      (next-iteration))
    (unless (= index
               (* cl-ds.common.rrb:+maximum-children-count+
                  (truncate index cl-ds.common.rrb:+maximum-children-count+)))
      (reduce-stack iterator index depth stack column))
    (setf (cl-ds.dicts.srrb:access-shift column) depth)
    (for stack-head = (first-elt stack))
    (setf (cl-ds.dicts.srrb:access-tree column)
          (if (or (null stack-head)
                  (~> stack-head
                      cl-ds.common.rrb:sparse-rrb-node-size
                      zerop))
              cl-ds.meta:null-bucket
              stack-head))
    (for tree-present? = (~> column
                             cl-ds.dicts.srrb:access-tree
                             cl-ds.meta:null-bucket-p
                             not))
    (setf (cl-ds.dicts.srrb:access-shift column) (if tree-present? depth 0)
          (cl-ds.dicts.srrb:access-tree-size column)
          (if tree-present?
              (cl-ds.common.rrb:sparse-rrb-tree-size stack-head
                                                     depth)
              0))
    (for tree-index-bound = (* #1=cl-ds.common.rrb:+maximum-children-count+
                               (ceiling (cl-ds.dicts.srrb:scan-index-bound column)
                                        #1#)))
    (for index-bound = (+ #1# tree-index-bound))
    (setf (cl-ds.dicts.srrb:access-tree-index-bound column) tree-index-bound
          (cl-ds.dicts.srrb:access-index-bound column) index-bound)))


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
           :column position
           :value value
           :format-control "Setting content of the column (column ~a) to :null is not allowed. Use ERASE! instead."
           :format-arguments (list position)))
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
  (+ (cl-ds.dicts.srrb:scan-index-bound column)
     (~> column
         cl-ds.dicts.srrb:access-tail-mask
         integer-length)))


(defmethod remove-nulls ((iterator sparse-material-column-iterator))
  (bind ((columns (read-columns iterator))
         (depth (~> (extremum columns #'>
                              :key #'cl-ds.dicts.srrb:access-shift)
                    cl-ds.dicts.srrb:access-shift))
         ((:flet unify-shift (column &aux (root (cl-ds.dicts.srrb:access-tree column))))
          (unless (cl-ds.meta:null-bucket-p root)
            (iterate
              (for i from (cl-ds.dicts.srrb:access-shift column) below depth)
              (for node
                   initially (cl-ds.dicts.srrb:access-tree column)
                   then (make-node iterator column 1
                                   :content (vector node)))
              (finally (setf (cl-ds.dicts.srrb:access-tree column) node))))))
    (cl-ds.utils:transform (read-transformation iterator) columns)
    (map nil #'unify-shift columns)
    (concatenate-trees iterator)
    (trim-depth iterator)
    (iterate
      (for column in-vector columns)
      (setf (cl-ds.dicts.srrb:access-tree-size column)
            (cl-ds.common.rrb:sparse-rrb-tree-size
             (cl-ds.dicts.srrb:access-tree column)
             (cl-ds.dicts.srrb:access-shift column))))
    nil))


(defmethod cl-ds:whole-range ((container sparse-material-column))
  (make-sparse-material-column-range container))


(defmethod cl-ds:across ((container sparse-material-column)
                         function)
  (ensure-functionf function)
  (bind ((shift (the fixnum (cl-ds.dicts.srrb:access-shift container)))
         (column-size (the fixnum (cl-ds.dicts.srrb:access-tree-index-bound container)))
         (root (cl-ds.dicts.srrb:access-tree container))
         (tail-mask (the fixnum (cl-ds.dicts.srrb:access-tail-mask container)))
         (tail (the (or null simple-vector) (cl-ds.dicts.srrb:access-tail container)))
         ((:flet map-tail ())
          (declare (type function function))
          (iterate
            (declare (type fixnum i))
            (for i from 0 below (integer-length tail-mask))
            (if (ldb-test (byte 1 i) tail-mask)
                (funcall function (aref tail i))
                (funcall function :null)))
          (return-from cl-ds:across container))
         ((:labels map-leaf (leaf index))
          (declare (type cl-ds.common.rrb:sparse-rrb-node leaf)
                   (type fixnum index)
                   (type function function))
          (iterate
            (declare (type fixnum i in))
            (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
            (for in from index)
            (unless (< in column-size)
              (map-tail))
            (if (cl-ds.common.rrb:sparse-rrb-node-contains leaf i)
                (funcall function (cl-ds.common.rrb:sparse-nref leaf i))
                (funcall function :null))))
         ((:labels map-nulls (level index))
          (declare (type function function)
                   (type fixnum level))
          (iterate
            (declare (type fixnum i in))
            (for in from index)
            (unless (< in column-size)
              (map-tail))
            (for i from 0
                 below (the fixnum (ash cl-ds.common.rrb:+maximum-children-count+
                                        (the fixnum (* cl-ds.common.rrb:+bit-count+ level)))))
            (funcall function :null)))
         ((:labels map-subtree (subtree level index))
          (declare (type cl-ds.common.rrb:sparse-rrb-node subtree)
                   (type function function)
                   (type fixnum level))
          (let ((level-1 (1- level))
                (content (cl-ds.common.rrb:sparse-rrb-node-content subtree))
                (c-index 0))
            (declare (type fixnum c-index level-1)
                     (type simple-vector content))
            (cl-ds.utils:cases ((zerop level-1))
              (iterate
                (declare (type fixnum i next-index byte-position))
                (with byte-position = (* 5 level))
                (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
                (for next-index = (dpb i (byte 5 byte-position) index))
                (if (cl-ds.common.rrb:sparse-rrb-node-contains subtree i)
                    (progn
                      (if (zerop level-1)
                          (map-leaf (svref content c-index)
                                    next-index)
                          (map-subtree (svref content c-index)
                                       level-1
                                       next-index))
                      (the fixnum (incf c-index)))
                    (map-nulls level-1 next-index)))))))
    (cond ((cl-ds.meta:null-bucket-p root) nil)
          ((zerop shift) (map-leaf root 0))
          (t (map-subtree root shift 0)))
    (map-tail)))


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
  (make-sparse-material-column-iterator
   :index (index iterator)
   :initial-index (index iterator)
   :stacks (~>> iterator read-stacks (map 'vector #'copy-array))
   :columns (read-columns iterator)
   :depths (~> iterator read-depths copy-array)
   :buffers (~> iterator read-buffers copy-array)
   :changes (~> iterator read-changes copy-array)
   :touched (~> iterator read-touched copy-array)
   :indexes (~> iterator read-indexes copy-array)
   :initialization-status (~> iterator
                              read-initialization-status
                              copy-array)))


(defmethod cl-ds:reset! ((iterator sparse-material-column-iterator))
  (change-leafs iterator)
  (clear-changes iterator)
  (clear-buffers iterator)
  (map-into (read-initialization-status iterator) (constantly nil))
  (map-into (read-touched iterator) (constantly nil))
  (setf (access-index iterator) (read-initial-index iterator))
  iterator)
