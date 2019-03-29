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
  (declare (type fixnum new-depth)
           (optimize (speed 3) (safety 0)))
  (iterate
    (declare (type fixnum index i length)
             (type (vector fixnum) depths)
             (type (vector t) stacks columns))
    (with index = (access-index iterator))
    (with depths = (read-depths iterator))
    (with columns = (read-columns iterator))
    (with stacks = (read-stacks iterator))
    (with length = (length depths))
    (for i from 0 below length)
    (for depth = (aref depths i))
    (when (>= depth new-depth)
      (next-iteration))
    (for stack = (aref stacks i))
    (for column = (aref columns i))
    (pad-stack iterator depth index new-depth stack column)
    (maxf (aref depths i) new-depth)))


(defun ensure-column-initialization (iterator column)
  (bind (((:slots %initialization-status %columns) iterator)
         (status %initialization-status)
         (length (length status)))
    (declare (type (simple-array boolean (*)) status))
    (unless (< -1 column length)
      (error 'no-such-column
             :bounds `(0 ,length)
             :argument 'column
             :value column
             :format-control "There is no such column."))
    (unless (aref status column)
      (setf (aref status column) t)
      (initialize-iterator-column
       (access-index iterator)
       (~> iterator read-columns (aref column))
       (~> iterator read-stacks (aref column))
       (~> iterator read-buffers (aref column))
       (~> iterator read-depths (aref column))
       (~> iterator read-touched (aref column))))))


(defun initialize-iterator-column (index column stack buffer shift touched)
  (unless touched
    (map-into stack (constantly nil))
    (setf (first-elt stack) (column-root column)))
  (move-stack shift index stack)
  (fill-buffer shift buffer stack))


(defun initialize-iterator-columns (iterator)
  (map nil (curry #'initialize-iterator-column (access-index iterator))
       (read-columns iterator)
       (read-stacks iterator)
       (read-buffers iterator)
       (read-depths iterator)
       (read-touched iterator)))


(defun index-promoted (old-index new-index)
  (declare (optimize (speed 3) (safety 0) (space 0) (debug 0))
           (type fixnum old-index new-index))
  (not (eql (ceiling (the fixnum (1+ old-index))
                     cl-ds.common.rrb:+maximum-children-count+)
            (ceiling (the fixnum (1+ new-index))
                     cl-ds.common.rrb:+maximum-children-count+))))


(defun children (nodes)
  (declare (optimize (debug 3)))
  (lret ((result (make-hash-table)))
    (iterate
      (for (index n) in-hashtable nodes)
      (iterate
        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
        (when (cl-ds.common.rrb:sparse-rrb-node-contains n i)
          (setf (gethash (child-index index i) result)
                (cl-ds.common.rrb:sparse-nref n i)))))
    (assert (= (~> result
                   cl-ds.utils:inverted-hash-table
                   hash-table-count)
               (hash-table-count result)))))


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
  (parents nil :type (or null concatenation-state))
  (columns #() :type vector))


(defun concatenation-state (iterator columns nodes parents)
  (bind (((:values masks max-index) (gather-masks nodes)))
    (assert (every (lambda (nodes)
                     (= (~> nodes
                            cl-ds.utils:inverted-hash-table
                            hash-table-count)
                        (hash-table-count nodes)))
                   nodes))
    (make-concatenation-state
     :changed-parents (map 'vector
                           (lambda (x)
                             (declare (ignore x))
                             (make-hash-table))
                           columns)
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
    (assert (= (~> nodes
                   (aref column)
                   cl-ds.utils:inverted-hash-table
                   hash-table-count)
               (hash-table-count (aref nodes column))))
    (gethash index (aref nodes column))))


(-> (setf node) ((or null cl-ds.common.rrb:sparse-rrb-node)
                 concatenation-state fixnum fixnum)
    (or null cl-ds.common.rrb:sparse-rrb-node))
(defun (setf node) (new-value state column index)
  (declare (optimize (debug 3)))
  (with-concatenation-state (state)
    (if (null new-value)
        (remhash index (aref nodes column))
        (setf (gethash index (aref nodes column)) new-value))
    (assert (= (~> nodes
                   (aref column)
                   cl-ds.utils:inverted-hash-table
                   hash-table-count)
               (hash-table-count (aref nodes column))))
    new-value))


(-> mask (concatenation-state fixnum) cl-ds.common.rrb:sparse-rrb-mask)
(defun mask (state index)
  (declare (type concatenation-state state)
           (type fixnum index))
  (with-concatenation-state (state)
    (gethash index masks 0)))


(defun (setf mask) (mask state index)
  (with-concatenation-state (state)
    (setf (gethash index masks) mask)))


(-> (setf parent-changed) (boolean concatenation-state fixnum fixnum) boolean)
(defun (setf parent-changed) (new-value state column parent-index)
  (unless (null state)
    (with-concatenation-state (state)
      (if new-value
          (setf (gethash parent-index (aref changed-parents column)) t)
          (remhash parent-index (aref changed-parents column)))
      new-value)))


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
     (occupied-space state index)))


(-> parent-index (fixnum) fixnum)
(defun parent-index (child-index)
  (ash child-index (- cl-ds.common.rrb:+bit-count+)))


(define-symbol-macro mask-bytes (byte cl-ds.common.rrb:+maximum-children-count+ 0))


(defun move-to-existing-column (state from to from-mask to-mask column-index)
  (declare (optimize (debug 3)))
  (with-concatenation-state (state)
    (bind ((column (aref columns column-index))
           (column-tag (cl-ds.common.abstract:read-ownership-tag column))
           (from-node (node state column-index from))
           (to-node (node state column-index to))
           (taken (logcount to-mask))
           (free-space (- cl-ds.common.rrb:+maximum-children-count+
                          taken))
           (from-content (cl-ds.common.rrb:sparse-rrb-node-content from-node))
           (from-size (cl-ds.common.rrb:sparse-rrb-node-size from-node))
           (element-type (array-element-type from-content))
           (from-owned (cl-ds.common.abstract:acquire-ownership
                        from-node column-tag))
           (to-owned (cl-ds.common.abstract:acquire-ownership
                      to-node column-tag))
           (to-content (cl-ds.common.rrb:sparse-rrb-node-content to-node))
           (real-to-size (cl-ds.common.rrb:sparse-rrb-node-size to-node))
           (real-to-mask (cl-ds.common.rrb:sparse-rrb-node-bitmask to-node))
           (real-from-mask (cl-ds.common.rrb:sparse-rrb-node-bitmask from-node))
           (shifted-from-mask (ldb mask-bytes (ash real-from-mask taken)))
           (shifted-count (logcount shifted-from-mask))
           (new-from-mask (ldb (byte cl-ds.common.rrb:+maximum-children-count+
                                     free-space)
                               real-from-mask))
           (new-to-mask (ldb mask-bytes
                             (logior real-to-mask shifted-from-mask)))
           (new-to-size (logcount new-to-mask)))
      (declare (type list from-node to-node))
      (assert (< new-from-mask real-from-mask))
      (assert (<= (logcount new-from-mask) (logcount from-mask)))
      (when (zerop new-from-mask)
        (setf (node state column-index from) nil))
      (if (and to-owned
               (>= (length to-content) new-to-size))
          (iterate
            (for j from 0 below from-size)
            (for i from (logcount real-to-mask) below (length to-content))
            (repeat shifted-count)
            (setf (aref to-content i) (aref from-content j))
            (finally (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask to-node)
                           new-to-mask)))
          (let ((new-content (make-array new-to-size
                                         :element-type element-type)))
            (assert (> new-to-size taken))
            (iterate
              (for i from 0 below real-to-size)
              (setf (aref new-content i) (aref to-content i)))
            (iterate
              (for i from real-to-size below new-to-size)
              (for j from 0)
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
               (with new-from-size = (logcount new-from-mask))
               (for i from (- from-size new-from-size) below from-size)
               (for j from 0 below new-from-size)
               (setf (aref from-content j) (aref from-content i))))
            (t (let* ((new-from (make-node iterator
                                           column new-from-mask
                                           :type element-type))
                      (new-content (cl-ds.common.rrb:sparse-rrb-node-content
                                    new-from)))
                 (iterate
                   (for i from shifted-count)
                   (for j from 0 below (logcount new-from-mask))
                   (setf (aref new-content j) (aref from-content i)))
                 (setf (node state column-index from) new-from)))))))


(-> move-children-in-column (concatenation-state
                             fixnum fixnum
                             cl-ds.common.rrb:sparse-rrb-mask
                             cl-ds.common.rrb:sparse-rrb-mask
                             fixnum)
    t)
(defun move-children-in-column (state from to from-mask
                                to-mask column-index)
  (declare (optimize (debug 3)))
  (with-concatenation-state (state)
    (bind ((from-parent (parent-index from))
           (to-parent (parent-index to))
           (column (aref columns column-index))
           (column-tag (cl-ds.common.abstract:read-ownership-tag column))
           (from-node (node state column-index from))
           (to-node (node state column-index to))
           (to-exists (not (null to-node)))
           (from-exists (not (null from-node)))
           (from-owned (cl-ds.common.abstract:acquire-ownership
                        from-node column-tag)))
      (declare (type list from-node to-node))
      (unless from-exists
        (return-from move-children-in-column nil))
      (setf (parent-changed state column-index from-parent) t
            (parent-changed state column-index to-parent) t)
      (if to-exists
          (move-to-existing-column state from to
                                   from-mask to-mask
                                   column-index)
          (setf (node state column-index from) nil
                (node state column-index to)
                (if from-owned
                    from-node
                    (cl-ds.common.rrb:deep-copy-sparse-rrb-node
                     from-node 0 column-tag))))
      nil)))


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
  (declare (optimize (debug 3)))
  (with-concatenation-state (state)
    (let* ((to-mask (mask state to))
           (from-mask (mask state from))
           (taken (logcount to-mask))
           (free-space (- cl-ds.common.rrb:+maximum-children-count+
                          taken))
           (required-space (logcount from-mask)))
      (declare (type fixnum required-space free-space)
               (type cl-ds.common.rrb:sparse-rrb-mask to-mask from-mask))
      (prog1
          (cond ((zerop required-space) 0)
                ((zerop free-space) 1)
                ((> free-space required-space)
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
                 1))
        (setf (mask state from) (ldb (byte cl-ds.common.rrb:+maximum-children-count+
                                           free-space)
                                     from-mask)
              (mask state to) (ldb mask-bytes
                                   (logior to-mask
                                           (ash from-mask taken))))))))


(defun shift-content (state)
  (iterate
    (with destination = 0)
    (for i from 1 to (concatenation-state-max-index state))
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


(defun update-parents (state column)
  (with-concatenation-state (state)
    (iterate
      (with tag = (~> columns
                      (aref column)
                      cl-ds.common.abstract:read-ownership-tag))
      (for (index changed) in-hashtable (aref changed-parents column))
      (assert changed)
      (for parent = (node parents column index))
      (for mask = 0)
      (iterate
        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
        (for child-index = (child-index index i))
        (for child = (node state column child-index))
        (unless (null child)
          (setf mask (dpb 1 (byte 1 i) mask))))
      (if (zerop mask)
          (setf (parent-changed parents column (parent-index index)) t
                (node parents column index) nil)
          (let ((new-content
                  (~>> parent
                       cl-ds.common.rrb:sparse-rrb-node-content
                       array-element-type
                       (make-array (logcount mask) :element-type _))))
            (iterate
              (with content-position = 0)
              (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
              (unless (ldb-test (byte 1 i) mask)
                (next-iteration))
              (for child-index = (child-index index i))
              (for child = (node state column child-index))
              (assert child)
              (setf (aref new-content content-position) child)
              (incf content-position))
            (assert
             (iterate
               (for child in-vector new-content)
               (for prev-child previous child)
               (always (not (eq child prev-child)))))
            (if (cl-ds.common.abstract:acquire-ownership parent tag)
                (setf (cl-ds.common.rrb:sparse-rrb-node-content parent)
                      new-content
                      (cl-ds.common.rrb:sparse-rrb-node-bitmask parent) mask)
                (let* ((parent-index (parent-index index))
                       (new-node (make-node iterator column mask
                                            :content new-content)))
                  (setf (parent-changed parents column parent-index) t
                        (node parents column index) new-node))))))))


(defun concatenate-trees (iterator)
  (declare (optimize (debug 3)))
  (bind ((columns (~>> iterator read-columns
                       (remove-if #'null _ :key #'column-root)))
         (depth (extremum columns #'>
                          :key #'cl-ds.dicts.srrb:access-shift))
         ((:labels impl (d nodes parent-state))
          (let ((current-state (concatenation-state iterator
                                                    columns
                                                    nodes
                                                    parent-state)))
            (unless (eql d depth)
              (impl (1+ d)
                    (map 'vector #'children nodes)
                    current-state))
            (shift-content current-state)
            (unless (null parent-state)
              (iterate
                (for i from 0 below (length nodes))
                (update-parents current-state i)))
            current-state))
         ((:flet pack-root-into-hashtable (element))
          (lret ((result (make-hash-table)))
            (setf (gethash 0 result) (cl-ds.dicts.srrb:access-tree element))))
         (roots (map 'vector #'pack-root-into-hashtable columns)))
    (impl 0 roots nil)
    (iterate
      (for column in-vector columns)
      (for root-table in-vector roots)
      (for root = (gethash 0 root-table))
      (setf (cl-ds.dicts.srrb:access-tree column)
            (if (null root)
                cl-ds.meta:null-bucket
                root)))))


(defun remove-nulls-in-trees (iterator)
  (declare (optimize (debug 3)))
  (bind ((columns (~>> iterator read-columns
                       (remove-if #'null _ :key #'column-root)))
         (depth (extremum columns #'>
                          :key #'cl-ds.dicts.srrb:access-shift))
         ((:flet truncate-mask (mask))
          (ldb (byte cl-ds.common.rrb:+maximum-children-count+ 0) mask))
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
            (assert (eql (logcount new-bitmask)
                         (logcount old-bitmask)))
            (unless (eql new-bitmask old-bitmask)
              (let* ((column (aref columns i))
                     (tag (cl-ds.common.abstract:read-ownership-tag column))
                     (owned (cl-ds.common.abstract:acquire-ownership node
                                                                     tag)))
                (unless owned
                  (setf node (cl-ds.common.rrb:deep-copy-sparse-rrb-node node
                                                                         0
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
              (when (ldb-test (byte 1 i) present-mask)
                (map-into next-nodes
                          (rcurry #'cl-ds.common.rrb:sparse-nref i)
                          nodes)
                (impl (1+ d) next-nodes)
                (map nil
                     (lambda (parent-node child-node)
                       (setf (cl-ds.common.rrb:sparse-nref parent-node i)
                             child-node))
                     nodes
                     next-nodes)))))
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
  (declare (type fixnum new-depth new-index)
           (optimize (speed 3) (safety 0)))
  (pad-stacks iterator new-depth)
  (iterate
    (declare (type (simple-array fixnum (*)) depths)
             (type (simple-array boolean (*)) initialization-status)
             (type simple-vector stacks)
             (type fixnum length i))
    (with stacks = (read-stacks iterator))
    (with depths = (read-depths iterator))
    (with initialization-status = (read-initialization-status iterator))
    (with length = (length stacks))
    (for i from 0 below length)
    (for stack = (aref stacks i))
    (for depth = (aref depths i))
    (for initialized = (aref initialization-status i))
    (when initialized
      (move-stack depth new-index stack)))
  (setf (access-index iterator) new-index))


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
  (declare (type simple-vector buffer)
           (optimize (speed 3)))
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
      (for v = (aref buffer i))
      (unless (eql v :null)
        (setf (aref new-content index) v
              (ldb (byte 1 i) bitmask) 1
              index (1+ index))))
    (make-node iterator column bitmask :content new-content)))


(declaim (inline change-leaf))
(defun change-leaf (iterator depth stack column change buffer)
  (cond ((every (curry #'eql :null) buffer)
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
  (declare (optimize (speed 3) (safety 0)))
  (let* ((initialization-status (read-initialization-status iterator))
         (depths (read-depths iterator))
         (stacks (read-stacks iterator))
         (columns (read-columns iterator))
         (changes (read-changes iterator))
         (buffers (read-buffers iterator))
         (length (length depths)))
    (declare (type simple-vector stacks columns changes buffers)
             (type (simple-array fixnum (*)) depths)
             (type (simple-array boolean (*)) initialization-status))
    (iterate
      (declare (type fixnum i))
      (for i from 0 below length)
      (when (aref initialization-status i)
        (unless (every #'null (aref changes i))
          (change-leaf iterator
                       (aref depths i)
                       (aref stacks i)
                       (aref columns i)
                       (aref changes i)
                       (aref buffers i)))))))


(defun copy-on-write-node (iterator parent child position tag column)
  (cond ((and (null parent) (null child))
         nil)
        ((null parent)
         (make-node iterator column (ash 1 position)
                    :content (lret ((vector (make-array 4)))
                               (setf (first-elt vector) child))))
        ((and (null child)
              (eql 1 (cl-ds.common.rrb:sparse-rrb-node-size parent)))
         nil)
        ((cl-ds.common.abstract:acquire-ownership parent tag)
         (if (null child)
             (when (cl-ds.common.rrb:sparse-rrb-node-contains parent position)
               (cl-ds.common.rrb:sparse-rrb-node-erase! parent position))
             (setf (cl-ds.common.rrb:sparse-nref parent position)
                   child))
         parent)
        (t (lret ((copy (cl-ds.common.rrb:deep-copy-sparse-rrb-node
                         parent
                         0
                         tag)))
             (if (null child)
                 (when (cl-ds.common.rrb:sparse-rrb-node-contains parent position)
                   (cl-ds.common.rrb:sparse-rrb-node-erase! parent position))
                 (setf (cl-ds.common.rrb:sparse-nref copy position)
                       child))))))


(defun reduce-stack (iterator index depth stack column)
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


(declaim (inline fill-buffer))
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
  (declare (optimize (speed 3) (safety 0)))
  (let ((depths (read-depths iterator))
        (initialization-status (read-initialization-status iterator))
        (buffers (read-buffers iterator))
        (stacks (read-stacks iterator)))
    (declare (type (simple-array fixnum (*)) depths)
             (type simple-vector buffers stacks)
             (type (simple-array boolean (*)) initialization-status))
    (map nil
         (lambda (d i b s)
           (when i
             (fill-buffer d b s)))
         depths
         initialization-status
         buffers
         stacks)))


(defun reduce-stacks (iterator)
  (declare (optimize (speed 3) (safety 0)))
  (let ((initialization-status (read-initialization-status iterator))
        (depths (read-depths iterator))
        (stacks (read-stacks iterator))
        (index (access-index iterator))
        (columns (read-columns iterator)))
    (declare (type simple-vector stacks columns)
             (type (simple-array fixnum (*)) depths)
             (type (simple-array boolean (*)) initialization-status))
    (map nil
         (lambda (i d s c)
           (when i (reduce-stack iterator index d s c)))
         initialization-status depths stacks columns)))


(defun clear-changes (iterator)
  (declare (optimize (speed 3) (safety 0)))
  (iterate
    (declare (type simple-vector changes)
             (type fixnum i length)
             (type (simple-array boolean (*)) initialization-status))
    (with changes = (read-changes iterator))
    (with initialization-status = (read-initialization-status iterator))
    (with length = (length changes))
    (for i from 0 below length)
    (for change = (aref changes i))
    (for initialized = (aref initialization-status i))
    (when initialized
      (map-into (the simple-vector change) (constantly nil)))))


(defun clear-buffers (iterator)
  (declare (optimize (speed 3) (safety 0)))
  (iterate
    (declare (type fixnum length i)
             (type (simple-array boolean (*)) initialization-status)
             (type simple-vector buffers))
    (with buffers = (read-buffers iterator))
    (with initialization-status = (read-initialization-status iterator))
    (with length = (length buffers))
    (for i from 0 below length)
    (for buffer = (aref buffers i))
    (for initialized = (aref initialization-status i))
    (when initialized
      (map-into (the simple-vector buffer) (constantly :null)))))


(defun sparse-material-column-at (column index)
  (declare (type sparse-material-column column))
  (check-type index non-negative-integer)
  (bind (((:values value found)
          (cl-ds.dicts.srrb:sparse-rrb-vector-at column index)))
    (if found value :null)))


(defun (setf sparse-material-column-at) (new-value column index)
  (declare (type sparse-material-column column))
  (check-type index non-negative-integer)
  (cl-ds.meta:position-modification #'(setf cl-ds:at)
                                    column
                                    column
                                    index
                                    :value new-value)
  new-value)


(defun column-root (column)
  (let ((root (cl-ds.dicts.srrb:access-tree column)))
    (if (cl-ds.meta:null-bucket-p root)
        nil
        root)))


(defun trim-depth-in-column (column)
  (bind (((:labels skip (node))
          (if (or (cl-ds.meta:null-bucket-p node)
                  (~> node
                      cl-ds.common.rrb:sparse-rrb-node-bitmask
                      (eql 1)
                      not))
              node
              (skip (cl-ds.common.rrb:sparse-nref node 0))))
         (tree-index-bound (cl-ds.dicts.srrb:scan-index-bound column))
         (index-bound (* #1=cl-ds.common.rrb:+maximum-children-count+
                         (1+ (ceiling tree-index-bound #1#))))
         (root (cl-ds.dicts.srrb:access-tree column)))
    (setf (cl-ds.dicts.srrb:access-tree-index-bound column) tree-index-bound
          (cl-ds.dicts.srrb:access-index-bound column) index-bound

          (cl-ds.dicts.srrb:access-shift column)
          (cl-ds.dicts.srrb:shift-for-position (1- tree-index-bound))

          (cl-ds.dicts.srrb:access-tree column) (skip root))))


(defun trim-depth (iterator)
  (map nil #'trim-depth-in-column (read-columns iterator)))


(defclass sparse-material-column-range (cl-ds:fundamental-forward-range)
  ((%iterator :initarg :iterator
              :accessor access-iterator)
   (%column :initarg :column
            :reader read-column)
   (%position :initarg :position
              :accessor access-position)
   (%initial-position :initarg :position
                      :reader read-initial-position)))


(defun range-iterator (range position)
  (lret ((iterator (~> range read-column list make-iterator)))
    (move-iterator iterator position)))


(defmethod cl-ds:reset! ((range sparse-material-column-range))
  (setf (access-iterator range) (range-iterator range
                                                (read-initial-position range))
        (access-position range) (read-initial-position range))
  range)


(defmethod cl-ds:clone ((range sparse-material-column-range))
  (make 'sparse-material-column
        :iterator (range-iterator range
                                  (access-position range))
        :column (read-column range)
        :position (access-position range)))


(defmethod cl-ds:consume-front ((range sparse-material-column-range))
  (let* ((iterator (access-iterator range))
         (column (read-column range))
         (position (access-position range))
         (more (< position (cl-ds:size column))))
    (values (if more
                (prog1
                    (iterator-at iterator 0)
                  (setf (access-position range) (1+ position))
                  (move-iterator iterator 1))
                nil)
            more)))


(defmethod cl-ds:drop-front ((range sparse-material-column-range)
                             count)
  (check-type count non-negative-fixnum)
  (let* ((column (read-column range))
         (count (clamp count 0 (- (cl-ds:size column)
                                  (access-position range)))))
    (when (zerop count)
      (return-from cl-ds:drop-front (values range count)))
    (move-iterator (access-iterator range) count)
    (values range count)))


(defmethod cl-ds:peek-front ((range sparse-material-column-range))
  (let* ((iterator (access-iterator range))
         (column (read-column range))
         (position (access-position range))
         (more (< position (column-size column))))
    (values (if more
                (iterator-at iterator 0)
                nil)
            more)))


(defun make-sparse-material-column-range (column)
  (make 'sparse-material-column-range
        :iterator (make-iterator `(,column))
        :column column
        :position 0))
