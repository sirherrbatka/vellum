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
  (lret ((result (make-hash-table)))
    (iterate
      (for (index n) in-hashtable nodes)
      (for parent-index = (ash index cl-ds.common.rrb:+bit-count+))
      (iterate
        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
        (when (cl-ds.common.rrb:sparse-rrb-node-contains n i)
          (setf (gethash (logior i parent-index) result)
                (cl-ds.common.rrb:sparse-nref n i)))))))


(defun mask (table index)
  (gethash index table 0))


(defun gather-masks (nodes)
  (iterate outer
    (with result = (make-hash-table))
    (for column in-vector nodes)
    (iterate
      (for (index n) in-hashtable column)
      (in outer (maximizing index into max-index))
      (for existing-mask = (cl-ds.common.rrb:sparse-rrb-node-bitmask n))
      (for mask = (gethash index result 0))
      (setf (gethash index result) (logior mask existing-mask)))
    (finally (return-from outer (values result max-index)))))


(defstruct concatenation-state
  iterator
  (changed-parents #() :type vector)
  (masks (make-hash-table) :type hash-table)
  (max-index 0 :type non-negative-fixnum)
  (nodes #() :type vector)
  (parents #() :type (or null vector))
  (columns #() :type vector))


(defun concatenation-state (iterator columns nodes parents)
  (bind (((:values masks max-index) (gather-masks nodes)))
    (make-concatenation-state
     :iterator iterator
     :masks masks
     :max-index max-index
     :nodes nodes
     :columns columns
     :parents parents)))


(defmacro with-concatenation-state ((state) &body body)
  `(bind (((:accessors (masks concatenation-state-masks)
                       (changed-parents concatenation-state-changed-parents)
                       (max-index concatenation-state-max-index)
                       (iterator concatenation-state-iterator)
                       (nodes concatenation-state-nodes)
                       (columns concatenation-state-columns)
                       (parents concatenation-state-parents)
                       (nodes-end concatenation-state-nodes-end))
           ,state))
     ,@body))


(-> node (concatenation-state fixnum fixnum)
    cl-ds.common.rrb:sparse-rrb-node)
(defun node (state column index)
  (declare (type concatenation-state state)
           (type fixnum index column))
  (with-concatenation-state (state)
    (gethash index (aref nodes column))))


(-> (setf node) ((or null cl-ds.common.rrb:sparse-rrb-node)
                 concatenation-state fixnum fixnum)
    (or null cl-ds.common.rrb:sparse-rrb-node))
(defun (setf node) (new-value state column index)
  (with-concatenation-state (state)
    (if (null new-value)
        (remhash index (aref nodes column))
        (setf (gethash index (aref nodes column)) new-value))))


(-> mask (concatenation-state fixnum) cl-ds.common.rrb:sparse-rrb-mask)
(defun mask (state index)
  (declare (type concatenation-state state)
           (type fixnum index))
  (with-concatenation-state (state)
    (gethash index masks 0)))


(-> (setf parent-changed) (boolean concatenation-state fixnum fixnum) boolean)
(defun (setf parent-changed) (new-value state column parent-index)
  (with-concatenation-state (state)
    (if new-value
        (setf (gethash parent-index (aref changed-parents column)) t)
        (remhash parent-index (aref changed-parents column)))
    new-value))


(-> parent-changed (concatenation-state fixnum fixnum) boolean)
(defun parent-changed (state column parent-index)
  (with-concatenation-state (state)
    (nth-value 0 (gethash parent-index (aref changed-parents column)))))


(-> occupied-space (concatenation-state fixnum) fixnum)
(defun occupied-space (state index)
  (declare (type concatenation-state state)
           (type fixnum index))
  (logcount (mask state index)))


(-> free-space (concatenation-state fixnum) fixnum)
(defun free-space (state index)
  (declare (type concatenation-state state)
           (type fixnum index))
  (- cl-ds.common.rrb:+maximum-children-count+
     (space state index)))


(-> parent-index (fixnum) fixnum)
(defun parent-index (child-index)
  (ash child-index (- cl-ds.common.rrb:+bit-count+)))


(-> move-children-in-column (concatenation-state
                             fixnum fixnum
                             cl-ds.common.rrb:sparse-rrb-mask
                             cl-ds.common.rrb:sparse-rrb-mask
                             fixnum)
    t)
(defun move-children-in-column (state from to from-mask
                                to-mask column-index)
  (declare (ignore from-mask))
  (with-concatenation-state (state)
    (bind ((from-parent (parent-index from))
           (to-parent (parent-index to))
           (column (aref columns column-index))
           (column-tag (cl-ds.common.abstract:read-ownership-tag column))
           (from-node (node state column-index from))
           (to-node (node state column-index to))
           (free-space (- cl-ds.common.rrb:+maximum-children-count+
                          (logcount to-mask)))
           (from-exists t) ; from is always supposed to be there
           (from-content (cl-ds.common.rrb:sparse-rrb-node-content from-node))
           (from-size (cl-ds.common.rrb:sparse-rrb-node-size from-node))
           (element-type (cl-ds.common.rrb:read-element-type from-content))
           (to-exists (not (null to-node)))
           (from-owned (and from-exists
                            (cl-ds.common.abstract:acquire-ownership
                             from-node column-tag)))
           (to-owned (and to-exists
                          (cl-ds.common.abstract:acquire-ownership
                           to-node column-tag))))
      (declare (type list from-node to-node))
      (setf (parent-changed state column-index from-parent) t
            (parent-changed state column-index to-parent) t)
      (if to-exists
          (let* ((to-content (cl-ds.common.rrb:sparse-rrb-node-content to-node))
                 (to-size (length to-content))
                 (real-to-mask (cl-ds.common.rrb:sparse-rrb-node-bitmask to-node))
                 (real-from-mask (cl-ds.common.rrb:sparse-rrb-node-bitmask from-node))
                 (shifted-from-mask (ldb (byte cl-ds.common.rrb:+maximum-children-count+ 0)
                                         (ash real-from-mask free-space)))
                 (shifted-count (logcount shifted-from-mask))
                 (new-from-mask (ash real-from-mask (- free-space)))
                 (new-to-mask (logior real-to-mask shifted-from-mask))
                 (new-to-size (logcount new-to-mask)))
            (when (zerop new-from-mask)
              (setf (node state column-index from) nil))
            (if (and to-owned (>= to-size new-to-size))
                (iterate
                  (for j from 0)
                  (for i from (logcount real-to-mask))
                  (repeat shifted-count)
                  (setf (aref to-content i) (aref from-content j))
                  (finally (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask to-node)
                                 new-to-mask)))
                (let ((new-content (make-array new-to-size
                                               :element-type element-type)))
                  (iterate
                    (for i from 0 below to-size)
                    (setf (aref new-content i) (aref to-content i)))
                  (iterate
                    (for i from to-size)
                    (for j from 0 below from-size)
                    (setf (aref new-content i) (aref from-content j)))
                  (setf (node state column-index to)
                        (make-node iterator column new-to-mask
                                   :content new-content))))
            (cond ((zerop new-from-mask)
                   (setf (node state column-index from) nil))
                  (from-owned
                   (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask from-node)
                         new-from-mask)
                   (iterate
                     (for i from shifted-count)
                     (for j from 0 below (logcount new-from-mask))
                     (setf (aref from-content j) (aref from-content i))))
                  (t (let* ((new-from (make-node iterator column new-from-mask
                                                 :type element-type))
                            (new-content (cl-ds.common.rrb:sparse-rrb-node-content
                                          new-from)))
                       (iterate
                         (for i from shifted-count)
                         (for j from 0 below (logcount new-from-mask))
                         (setf (aref new-content j) (aref from-content i)))
                       (setf (node state column-index from) new-from)))))
          (setf (node state column-index to)
                (if from-owned
                    from-node
                    (cl-ds.common.rrb:deep-copy-sparse-rrb-node
                     from-node column-tag))
                (node state column-index from) nil)))))


(-> move-children-in-columns (concatenation-state
                              fixnum fixnum
                              cl-ds.common.rrb:sparse-rrb-mask
                              cl-ds.common.rrb:sparse-rrb-mask)
    t)
(defun move-children-in-columns (state from to from-mask to-mask)
  (iterate
    (with columns = (concatenation-state-columns state))
    (for i from 0 below (length columns))
    (move-children-in-column state from to from-mask to-mask i)))


(defun move-children (state from to)
  (with-concatenation-state (state)
    (let* ((to-mask (mask state to))
           (from-mask (mask state from))
           (free-space (- cl-ds.common.rrb:+maximum-children-count+
                          (logcount to-mask)))
           (required-space (logcount from-mask)))
      (declare (type fixnum required-space free-space)
               (type cl-ds.common.rrb:sparse-rrb-mask to-mask from-mask))
      (cond ((zerop required-space) 0)
            ((zerop free-space) 1)
            ((>= free-space required-space)
             (move-children-in-columns state
                                       from to
                                       from-mask
                                       to-mask)
             0)
            (t
             (move-children-in-columns state
                                       from to
                                       from-mask
                                       to-mask)
             1)))))


(defun shift-content (iterator columns nodes parents)
  (iterate
    (with destination = 0)
    (with state = (concatenation-state iterator columns nodes parents))
    (for i from 1 below (concatenation-state-max-index state))
    (iterate
      (for difference = (move-children state i destination))
      (until (zerop difference))
      (incf destination difference)
      (until (eql destination i)))
    (finally (return state))))


(-> child-index (fixnum fixnum) fixnum)
(defun child-index (parent-index child-position)
  (logior (ash parent-index cl-ds.common.rrb:+bit-count+)
          child-position))


(defun update-parents (state current-state column)
  (with-concatenation-state (state)
    (iterate
      (with column-parents = (aref parents column))
      (with tag = (~> columns
                      (aref column)
                      cl-ds.common.abstract:read-ownership-tag))
      (for (index changed) in-hashtable (aref changed-parents column))
      (for node = (gethash index column-parents))
      (for mask = 0)
      (iterate
        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
        (for child-index = (child-index index i))
        (for child = (node state column child-index))
        (setf mask (dpb 1 (byte 1 i) mask)))
      (if (zerop mask)
          (progn
            (setf (parent-changed current-state column
                                  (parent-index index))
                  t)
            (remhash index column-parents))
          (let ((new-content
                  (~>> node
                       cl-ds.common.rrb:sparse-rrb-node-content
                       array-element-type
                       (make-array (logcount mask) :element-type _))))
            (iterate
              (with new-content =
                    (~>> node
                         cl-ds.common.rrb:sparse-rrb-node-content
                         array-element-type
                         (make-array (logcount mask) :element-type _)))
              (with index = 0)
              (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
              (unless (ldb-test (byte 1 i) mask)
                (next-iteration))
              (for child-index = (child-index index i))
              (for child = (node state column child-index))
              (setf (aref new-content index) child)
              (incf index))
            (if (cl-ds.common.abstract:acquire-ownership node tag)
                (setf (cl-ds.common.rrb:sparse-rrb-node-content node)
                      new-content)
                (let* ((parent-index (parent-index index))
                       (new-node (make-node iterator column mask
                                            :content new-content)))
                  (setf (parent-changed current-state column parent-index) t
                        (node current-state column index) new-node))))))))


(defun concatenate-trees (iterator)
  (bind ((columns (~>> iterator read-columns
                       (remove-if #'null _ :key #'column-root)))
         (depth (access-depth iterator))
         ((:labels impl (d nodes parents))
          (unless (eql d depth)
            (let ((state
                    (impl (1+ d)
                          (map 'vector #'children nodes)
                          nodes))
                  (current-state (shift-content iterator columns
                                                nodes parents)))
              (unless (null parents)
                (iterate
                  (for i from 0 below (length nodes))
                  (update-parents state current-state i)))
              current-state)))
         ((:flet pack-root-into-hashtable (element))
          (lret ((result (make-hash-table)))
            (setf (gethash 0 result)
                  (cl-ds.dicts.srrb:access-tree element))))
         (roots (map 'vector #'pack-root-into-hashtable columns)))
    (impl 0 roots nil)
    (iterate
      (for column in-vector columns)
      (for root-table in-vector roots)
      (for root = (gethash 0 root-table))
      (setf (cl-ds.dicts.srrb:access-tree column) root))))


(defun remove-nulls-in-trees (iterator)
  (declare (optimize (debug 3)))
  (bind ((columns (~>> iterator read-columns
                       (remove-if #'null _ :key #'column-root)))
         (depth (access-depth iterator))
         ((:flet truncate-mask (mask))
          (ldb (byte cl-ds.common.rrb:+maximum-children-count+ 0) mask))
         ((:flet truncate-mask-to (mask removed-count))
          (ldb (byte (- cl-ds.common.rrb:+maximum-children-count+
                        removed-count)
                     0)
               mask))
         ((:flet missing-bitmask (node))
          (~> node
              cl-ds.common.rrb:sparse-rrb-node-bitmask
              lognot
              truncate-mask))
         ((:labels impl (d nodes))
          (iterate
            (with missing-mask = (reduce #'logand nodes
                                         :key #'missing-bitmask))
            (with missing-count = (logcount missing-mask))
            (for node in-vector nodes)
            (for i from 0)
            (for old-bitmask = (cl-ds.common.rrb:sparse-rrb-node-bitmask
                                node))
            (for new-bitmask = (~> (logior old-bitmask missing-mask)
                                   (ash (- missing-count))
                                   truncate-mask))
            (format t "~b:~b:~b~%"
                    old-bitmask new-bitmask missing-mask)
            (assert (eql (logcount new-bitmask)
                         (logcount old-bitmask)))
            (unless (eql new-bitmask old-bitmask)
              (let* ((column (aref columns i))
                     (tag (cl-ds.common.abstract:read-ownership-tag column))
                     (owned (cl-ds.common.abstract:acquire-ownership node
                                                                     tag)))
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


(defun trim-depth (iterator)
  (bind ((columns (read-columns iterator))
         ((:labels skip (node))
          (if (or (cl-ds.meta:null-bucket-p node)
                  (~> node
                      cl-ds.common.rrb:sparse-rrb-node-bitmask
                      (eql 1)
                      not))
              node
              (skip (cl-ds.common.rrb:sparse-nref node 0)))))
    (iterate
      (for column in-vector columns)
      (for tree-index-bound = (cl-ds.dicts.srrb:scan-index-bound column))
      (maximize tree-index-bound into maximum-size)
      (for index-bound = (+ cl-ds.common.rrb:+maximum-children-count+
                            tree-index-bound))
      (for root = (cl-ds.dicts.srrb:access-tree column))
      (setf (cl-ds.dicts.srrb:access-tree-index-bound column) tree-index-bound
            (cl-ds.dicts.srrb:access-index-bound column) index-bound
            (cl-ds.dicts.srrb:access-tree column) (skip root))
      (finally
       (iterate
         (for column in-vector columns)
         (setf (access-column-size column) maximum-size))))))
