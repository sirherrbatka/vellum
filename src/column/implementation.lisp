(in-package #:vellum.column)


(defmethod column-type ((column sparse-material-column))
  (cl-ds:type-specialization column))


(defmethod column-at ((column sparse-material-column) index)
  (sparse-material-column-at column index))


(defmethod (setf column-at) (new-value (column sparse-material-column) index)
  (setf (sparse-material-column-at column index) new-value))


(-> iterator-at (sparse-material-column-iterator integer) t)
(declaim (inline iterator-at))
(defun iterator-at (iterator column)
  (declare (optimize (speed 3) (safety 0) (debug 0) (compilation-speed 0) (space 0)))
  (ensure-column-initialization iterator column)
  (bind ((buffers (read-buffers iterator))
         (offset (offset (index iterator))))
    (declare (type simple-vector buffers))
    (~> (the iterator-buffer (aref buffers column)) (aref offset))))


(defun (setf iterator-at) (new-value iterator column)
  (declare (optimize (speed 3)))
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


(defmethod in-existing-content ((iterator sparse-material-column-iterator))
  (< (access-index iterator)
     (reduce #'max
             (read-columns iterator)
             :key #'column-size)))


(-> move-iterator-to (sparse-material-column-iterator non-negative-fixnum) t)
(defun move-iterator-to (iterator new-index)
  (declare (optimize (speed 3) (safety 0) (debug 0) (compilation-speed 0) (space 0)))
  (bind ((index (access-index iterator))
         (new-depth (~> new-index
                        integer-length
                        (ceiling cl-ds.common.rrb:+bit-count+)
                        1-))
         (promoted (index-promoted index new-index)))
    (declare (type fixnum index new-depth)
             (type boolean promoted))
    (unless promoted
      (setf (access-index iterator) new-index)
      (return-from move-iterator-to nil))
    (let* ((initialization-status (read-initialization-status iterator))
           (depths (read-depths iterator))
           (stacks (read-stacks iterator))
           (columns (read-columns iterator))
           (changes (read-changes iterator))
           (buffers (read-buffers iterator))
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
        (for not-changed = (every #'null (aref changes i)))
        (unless not-changed
          (change-leaf iterator
                       (aref depths i)
                       (aref stacks i)
                       (aref columns i)
                       (aref changes i)
                       (aref buffers i))
          (iterate
            (declare (type iterator-change change)
                     (type fixnum i))
            (with change = (aref changes i))
            (for i from 0 below (length change))
            (setf (aref change i) nil))
          (iterate
            (declare (type iterator-buffer buffer)
                     (type fixnum i))
            (with buffer = (aref buffers i))
            (for i from 0 below (length buffer))
            (setf (aref buffer i) :null))
          (reduce-stack iterator index
                        (aref depths i)
                        (aref stacks i)
                        (aref columns i)))
        (when (< (aref depths i) new-depth)
          (pad-stack iterator (aref depths i) index new-depth
                     (aref stacks i) (aref columns i))
          (maxf (aref depths i) new-depth))
        (if not-changed
            (setf (aref initialization-status i) nil)
            (progn
              (move-stack (aref depths i)
                          new-index
                          (aref stacks i))
              (fill-buffer (aref depths i)
                           (aref buffers i)
                           (aref stacks i)))))
      (setf (access-index iterator) new-index))
    nil))


(-> move-iterator (sparse-material-column-iterator non-negative-fixnum) t)
(defun move-iterator (iterator times)
  (declare (optimize (speed 3) (safety 0) (debug 0) (compilation-speed 0) (space 0)))
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
  (cl-ds.dicts.srrb:transactional-insert-tail!
   column (cl-ds.common.abstract:read-ownership-tag column))
  (let ((max-shift cl-ds.common.rrb:+maximal-shift+)
        (max-children-count cl-ds.common.rrb:+maximum-children-count+))
    (make-sparse-material-column-iterator
     :index (index iterator)
     :initial-index (index iterator)
     :touched (~>> iterator read-touched (into-vector-copy nil))
     :initialization-status (~>> iterator
                                 read-initialization-status
                                 (into-vector-copy nil))
     :columns (~>> iterator read-columns (into-vector-copy column))
     :changes (~>> iterator read-changes
                   (into-vector-copy (make-array max-children-count
                                                 :element-type 'boolean
                                                 :initial-element nil)))
     :stacks (~>> iterator read-stacks
                  (into-vector-copy (make-array max-shift
                                                :initial-element nil)))
     :buffers (~>> iterator read-buffers
                   (into-vector-copy (make-array max-children-count
                                                 :initial-element :null)))
     :depths (~>> iterator read-depths
                  (into-vector-copy (cl-ds.dicts.srrb:access-shift column))))))


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
     :stacks stacks
     :columns columns
     :changes changes
     :buffers buffers
     :touched touched
     :transformation transformation
     :depths depths)))


(defmethod finish-iterator ((iterator sparse-material-column-iterator))
  (change-leafs iterator)
  (reduce-stacks iterator)
  (iterate
    (for column in-vector (read-columns iterator))
    (for touched in-vector (read-touched iterator))
    (for column-size = (column-size column))
    (for depth in-vector (read-depths iterator))
    (setf (cl-ds.dicts.srrb:access-shift column) depth)
    (for stack in-vector (read-stacks iterator))
    (unless touched
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
           :column position
           :value value
           :format-control "Setting content of the column (column ~a) to :null is not allowed. Use ERASE! instead."
           :format-arguments position))
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
         ((:flet unify-shift (column))
          (iterate
            (for i from (cl-ds.dicts.srrb:access-shift column) below depth)
            (for node
                 initially (cl-ds.dicts.srrb:access-tree column)
                 then (make-node iterator column 1
                                 :content (vector node)))
            (finally (setf (cl-ds.dicts.srrb:access-tree column) node)))))
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
          (declare (optimize (speed 3) (safety 0) (debug 0) (space 0)
                             (compilation-speed 0))
                   (type function function))
          (iterate
            (declare (type fixnum i))
            (for i from 0 below (integer-length tail-mask))
            (if (ldb-test (byte 1 i) tail-mask)
                (funcall function (aref tail i))
                (funcall function :null)))
          (return-from cl-ds:across container))
         ((:labels map-leaf (leaf index))
          (declare (optimize (speed 3) (safety 0) (debug 0)
                             (space 0) (compilation-speed 0))
                   (type cl-ds.common.rrb:sparse-rrb-node leaf)
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
          (declare (optimize (speed 3) (safety 0) (debug 0) (space 0)
                             (compilation-speed 0))
                   (type function function)
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
          (declare (optimize (speed 3) (safety 0) (debug 0) (space 0)
                             (compilation-speed 0))
                   (type cl-ds.common.rrb:sparse-rrb-node subtree)
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
   :initialization-status (~> iterator read-initialization-status
                              copy-array)))


(defmethod cl-ds:reset! ((iterator sparse-material-column-iterator))
  (change-leafs iterator)
  (clear-changes iterator)
  (clear-buffers iterator)
  (map-into (read-initialization-status iterator) (constantly nil))
  (map-into (read-touched iterator) (constantly nil))
  (setf (access-index iterator) (read-initial-index iterator))
  iterator)
