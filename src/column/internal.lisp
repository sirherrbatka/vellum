(in-package #:cl-df.column)


(defun validate-iterator-position (iterator)
  (bind (((:slots %index %total-length) iterator))
    (unless (< %index %total-length)
      (error 'index-out-of-column-bounds
             :bounds `(0 ,%total-length)
             :value %index
             :text "Column index out of column bounds."))))


(defun offset (index)
  (declare (type fixnum index))
  (logandc2 index cl-ds.common.rrb:+tail-mask+))


(defun tree-index (index)
  (declare (type fixnum index))
  (logand index cl-ds.common.rrb:+tail-mask+))


(defun pad-stack (depth index new-depth stack column)
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
    (for node = (cl-ds.common.rrb:make-sparse-rrb-node
                 :ownership-tag tag
                 :content (vector prev-node)
                 :bitmask (ash 1 offset)))
    (setf prev-node node
          (aref stack j) node))
  (first-elt stack))


(defun pad-stacks (iterator new-depth)
  (map nil
       (curry #'pad-stack
              (access-depth iterator)
              (access-index iterator)
              new-depth)
       (read-stacks iterator)
       (read-columns iterator))
  (setf (access-depth iterator) new-depth))


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


(defun make-leaf (column old-node change buffer)
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
    (cl-ds.common.rrb:make-sparse-rrb-node
     :ownership-tag (cl-ds.common.abstract:read-ownership-tag column)
     :bitmask bitmask
     :content new-content)))


(defun change-leaf (depth stack column change buffer)
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
                     (make-leaf column old-node change buffer))
               (mutate-leaf column old-node change buffer)))))
  nil)


(defun change-leafs (iterator)
  (map nil
       (curry #'change-leaf
              (access-depth iterator))
       (read-stacks iterator)
       (read-columns iterator)
       (read-changes iterator)
       (read-buffers iterator)))


(defun copy-on-write-node (parent child position tag)
  (cond ((and (null parent) (null child))
         nil)
        ((null parent)
         (cl-ds.common.rrb:make-sparse-rrb-node
          :ownership-tag tag
          :content (vector child)
          :bitmask (ash 1 position)))
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


(defun reduce-stack (depth index stack column)
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
    (for new-node = (copy-on-write-node node prev-node position tag))
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
