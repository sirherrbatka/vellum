(in-package #:cl-df.column)


(declaim (inline make-node))
(defun make-node (iterator column bitmask
                  &key
                    type
                    (content (make-array
                              (logcount bitmask)
                              :element-type (or type (column-type column))))
                    (tag (cl-ds.common.abstract:read-ownership-tag column)))
  (declare (ignore iterator))
  (cl-ds.common.rrb:make-sparse-rrb-node
   :ownership-tag tag
   :content content
   :bitmask bitmask))


(defun offset (index)
  (declare (type fixnum index))
  (logandc2 index cl-ds.common.rrb:+tail-mask+))


(defun tree-index (index)
  (declare (type fixnum index))
  (logand index cl-ds.common.rrb:+tail-mask+))


(defun pad-stack (iterator depth index new-depth stack column)
  (iterate
    (with prev-node = (aref stack 0))
    (with tag = (cl-ds.common.abstract:read-ownership-tag column))
    (repeat (- new-depth depth))
    (for j from 0)
    (for byte
         from (* new-depth cl-ds.common.rrb:+bit-count+)
         downto 0
         by cl-ds.common.rrb:+bit-count+)
    (for offset = (ldb (byte cl-ds.common.rrb:+bit-count+ byte)
                       index))
    (for node = (make-node
                 iterator column (ash 1 offset)
                 :tag tag
                 :content (vector prev-node)))
    (setf prev-node node
          (aref stack j) node))
  (first-elt stack))


(defun pad-stacks (iterator new-depth)
  (map nil
       (curry #'pad-stack
              iterator
              (access-depth iterator)
              (access-index iterator)
              new-depth)
       (read-stacks iterator)
       (read-columns iterator))
  (setf (access-depth iterator) new-depth))


(defun initialize-iterator-column (column stack buffer)
  (map-into stack (constantly nil))
  (setf (first-elt stack) (column-root column))
  (let ((shift (cl-ds.dicts.srrb:access-shift column)))
    (move-stack shift 0 stack)
    (fill-buffer shift buffer stack)))


(defun initialize-iterator-columns (iterator)
  (map nil #'initialize-iterator-column
       (read-columns iterator)
       (read-stacks iterator)
       (read-buffers iterator)))


(defun index-promoted (old-index new-index)
  (not (eql (ceiling (1+ old-index)
                     cl-ds.common.rrb:+maximum-children-count+)
            (ceiling (1+ new-index)
                     cl-ds.common.rrb:+maximum-children-count+))))


(defun children (nodes)
  (lret ((result (vect)))
    (iterate
      (for (index . n) in-vector nodes)
      (for parent-index = (ash index cl-ds.common.rrb:+bit-count+))
      (iterate
        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
        (when (cl-ds.common.rrb:sparse-rrb-node-contains n i)
          (vector-push-extend (list* (logior i parent-index)
                                     (cl-ds.common.rrb:sparse-nref n i))
                              result))))))


(defun gather-masks (nodes)
  (lret ((result (make-hash-table)))
    (iterate
      (for column in-vector nodes)
      (iterate
        (for (index . n) in-vector column)
        (for existing-mask = (cl-ds.common.abstract:read-ownership-tag n))
        (for mask = (gethash index result existing-mask))
        (setf (gethash index result) (logior mask existing-mask))))))


(defun shift-content (nodes parents)
  cl-ds.utils:todo)


(defun concatenate-trees (iterator)
  (bind ((columns (~>> iterator read-columns
                       (remove-if #'null _ :key #'column-root)))
         (depth (access-depth iterator))
         ((:labels impl (d nodes parents))
          (unless (eql d depth)
            (impl (1+ d)
                  (children (map 'vector #'children nodes))
                  nodes))
          (shift-content nodes parents)))))


(defun remove-nulls-in-trees (iterator)
  (bind ((columns (~>> iterator read-columns
                       (remove-if #'null _ :key #'column-root)))
         (depth (access-depth iterator))
         ((:flet missing-bitmask (node))
          (~> node
              cl-ds.common.rrb:sparse-rrb-node-bitmask
              lognot))
         ((:labels impl (d nodes))
          (iterate
            (with missing-mask = (reduce #'logand nodes
                                         :key #'missing-bitmask))
            (for node in-vector nodes)
            (for i from 0)
            (for old-bitmask = (cl-ds.common.rrb:sparse-rrb-node-bitmask
                                node))
            (for this-missing = (lognot old-bitmask))
            (for unique-missing = (logandc2 this-missing missing-mask))
            (for new-bitmask = (logand (logxor old-bitmask missing-mask)
                                       unique-missing))
            (assert (eql (logcount new-bitmask)
                         (logcount old-bitmask)))
            (unless (eql new-bitmask old-bitmask)
              (let* ((column (aref columns i))
                     (tag (cl-ds.common.abstract:read-ownership-tag column))
                     (owned (cl-ds.common.abstract:acquire-ownership node tag)))
                (unless owned
                  (setf node (cl-ds.common.rrb:deep-copy-sparse-rrb-node node
                                                                         tag)
                        (aref nodes i) node))
                (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask node)
                      new-bitmask))))
          (when-let* ((subtree (not (eql d depth)))
                      (present-mask (reduce
                                     #'logand nodes
                                     :key #'cl-ds.common.rrb:sparse-rrb-node-bitmask))
                      (more-to-do (not (zerop present-mask))))
            (iterate
              (with next-nodes = (copy-array nodes))
              (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
              (when (ldb-test (byte 1 i) present-mask))
              (map-into next-nodes
                        (rcurry #'cl-ds.common.rrb:sparse-nref i)
                        nodes)
              (impl (1+ d) next-nodes)
              (map nil
                   (lambda (parent-node child-node)
                     (setf (cl-ds.common.rrb:sparse-nref parent-node i)
                           child-node))
                   nodes
                   next-nodes))))
         (roots (map 'vector #'column-root columns)))
    (impl 0 roots)
    (iterate
      (for root in-vector roots)
      (for column in-vector columns)
      (setf (cl-ds.dicts.srrb:access-tree column) root))))


(defun move-stack (depth new-index stack)
  (iterate outer
    (with node = (aref stack 0))
    (for i from 1 to depth)
    (for byte
         from (* depth cl-ds.common.rrb:+bit-count+)
         downto 0
         by cl-ds.common.rrb:+bit-count+)
    (for offset = (ldb (byte cl-ds.common.rrb:+bit-count+ byte) new-index))
    (for present = (cl-ds.common.rrb:sparse-rrb-node-contains node offset))
    (unless present
      (iterate
        (for j from i to depth)
        (setf (aref stack j) nil))
      (leave))
    (for child = (cl-ds.common.rrb:sparse-nref node offset))
    (setf node child)
    (unless (eq child (aref stack i))
      (setf (aref stack i) child)))
  (first-elt stack))


(defun move-stacks (iterator new-index new-depth)
  (let* ((depth (access-depth iterator)))
    (when (> new-depth depth)
      (pad-stacks iterator new-depth))
    (iterate
      (for stack in-vector (read-stacks iterator))
      (move-stack depth new-index stack))
    (setf (access-index iterator) new-index)))


(defun mutate-leaf (column old-node change buffer)
  (let* ((new-size (- cl-ds.common.rrb:+maximum-children-count+
                      (count :null buffer)))
         (old-content (cl-ds.common.rrb:sparse-rrb-node-content old-node))
         (old-size (array-dimension old-content 0))
         (bitmask 0)
         (new-content (if (>= old-size new-size)
                          old-content
                          (make-array new-size
                                      :element-type (column-type column)))))
    (declare (type simple-vector old-content)
             (type fixnum old-size new-size))
    (iterate
      (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
      (for changed in-vector change)
      (unless (or changed
                  (not (cl-ds.common.rrb:sparse-rrb-node-contains old-node
                                                                  i)))
        (setf (aref buffer i) (cl-ds.common.rrb:sparse-nref old-node i))))
    (iterate
      (with index = 0)
      (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
      (for v in-vector buffer)
      (unless (eql v :null)
        (setf (aref new-content index) v
              (ldb (byte 1 i) bitmask) 1
              index (1+ index))))
    (setf (cl-ds.common.rrb:sparse-rrb-node-content old-node) new-content
          (cl-ds.common.rrb:sparse-rrb-node-bitmask old-node) bitmask)))


(defun make-leaf (iterator column old-node change buffer)
  (unless (null old-node)
    (iterate
      (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
      (for changed in-vector change)
      (unless (or changed
                  (not (cl-ds.common.rrb:sparse-rrb-node-contains old-node
                                                                  i)))
        (setf (aref buffer i) (cl-ds.common.rrb:sparse-nref old-node i)))))
  (let* ((new-size (- cl-ds.common.rrb:+maximum-children-count+
                      (count :null buffer)))
         (new-content (make-array new-size
                                  :element-type (column-type column)))
         (bitmask 0))
    (iterate
      (with index = 0)
      (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
      (for v in-vector buffer)
      (unless (eql v :null)
        (setf (aref new-content index) v
              (ldb (byte 1 i) bitmask) 1
              index (1+ index))))
    (make-node iterator column bitmask :content new-content)))


(defun change-leaf (iterator depth stack column change buffer)
  (cond ((every #'null change)
         (return-from change-leaf nil))
        ((every (curry #'eql :null) buffer)
         (setf (aref stack depth) nil))
        (t
         (let* ((tag (cl-ds.common.abstract:read-ownership-tag column))
                (old-node (aref stack depth)))
           (if (or (null old-node)
                   (not (cl-ds.common.abstract:acquire-ownership old-node
                                                                 tag)))
               (setf (aref stack depth)
                     (make-leaf iterator column old-node change buffer))
               (mutate-leaf column old-node change buffer)))))
  nil)


(defun change-leafs (iterator)
  (map nil
       (curry #'change-leaf
              iterator
              (access-depth iterator))
       (read-stacks iterator)
       (read-columns iterator)
       (read-changes iterator)
       (read-buffers iterator)))


(defun copy-on-write-node (iterator parent child position tag column)
  (cond ((and (null parent) (null child))
         nil)
        ((null parent)
         (make-node iterator column (ash 1 position)
                    :content (vector child)))
        ((and (null child)
              (eql 1 (cl-ds.common.rrb:sparse-rrb-node-size parent)))
         nil)
        ((cl-ds.common.abstract:acquire-ownership parent tag)
         (if (null child)
             (cl-ds.common.rrb:sparse-rrb-node-erase! parent position)
             (setf (cl-ds.common.rrb:sparse-nref parent position)
                   child))
         parent)
        (t (lret ((copy (cl-ds.common.rrb:deep-copy-sparse-rrb-node
                         parent
                         tag)))
             (if (null child)
                 (cl-ds.common.rrb:sparse-rrb-node-erase! copy position)
                 (setf (cl-ds.common.rrb:sparse-nref copy position)
                       child))))))


(defun reduce-stack (iterator depth index stack column)
  (iterate
    (with tag = (cl-ds.common.abstract:read-ownership-tag column))
    (with prev-node = (aref stack depth))
    (for i from (1- depth) downto 0)
    (for bits
         from cl-ds.common.rrb:+bit-count+
         by cl-ds.common.rrb:+bit-count+)
    (for node = (aref stack i))
    (for position = (ldb (byte cl-ds.common.rrb:+bit-count+ bits)
                         index))
    (for new-node = (copy-on-write-node iterator node prev-node
                                        position tag column))
    (until (eql node new-node))
    (setf prev-node new-node
          (aref stack i) new-node))
  (first-elt stack))


(defun fill-buffer (depth buffer stack)
  (let ((node (aref stack depth)))
    (when (null node)
      (return-from fill-buffer nil))
    (iterate
      (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
      (for present = (cl-ds.common.rrb:sparse-rrb-node-contains node i))
      (when present
        (setf (aref buffer i) (cl-ds.common.rrb:sparse-nref node i))))
    node))


(defun fill-buffers (iterator)
  (map nil
       (curry #'fill-buffer (access-depth iterator))
       (read-buffers iterator)
       (read-stacks iterator)))


(defun reduce-stacks (iterator)
  (map nil
       (curry #'reduce-stack
              iterator
              (access-depth iterator)
              (access-index iterator))
       (read-stacks iterator)
       (read-columns iterator)))


(defun clear-changes (iterator)
  (iterate
    (for change in-vector (read-changes iterator))
    (map-into change (constantly nil))))


(defun clear-buffers (iterator)
  (iterate
    (for buffer in-vector (read-buffers iterator))
    (map-into buffer (constantly :null))))


(defun sparse-material-column-at (column index)
  (declare (type sparse-material-column column))
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


(defun (setf sparse-material-column-at) (new-value column index)
  (declare (type sparse-material-column column))
  (check-type index integer)
  (let ((column-size (access-column-size column)))
    (unless (< -1 index column-size)
      (error 'index-out-of-column-bounds
             :bounds `(0 ,column-size)
             :value index
             :argument 'index
             :text "Column index out of column bounds."))
    (cl-ds.meta:position-modification #'(setf cl-ds:at)
                                      column
                                      column
                                      index
                                      :value new-value)
    new-value))


(defun column-root (column)
  (let ((root (cl-ds.dicts.srrb:access-tree column)))
    (if (cl-ds.meta:null-bucket-p root)
        nil
        root)))
