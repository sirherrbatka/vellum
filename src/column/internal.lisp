(cl:in-package #:vellum.column)


(defstruct concatenation-state
  iterator
  (changed-parents #() :type vector)
  (masks (make-hash-table) :type hash-table) ; logior from all masks on a tree level, see gather-masks function
  (max-index 0 :type non-negative-fixnum)
  (nodes #() :type vector)
  (parents nil :type (or null concatenation-state))
  (columns #() :type simple-vector))


(defun make-node (iterator column bitmask
                  &key
                    type
                    (length (logcount bitmask))
                    (content (make-array
                              length
                              :element-type (or type (column-type column))))
                    (tag (cl-ds.common.abstract:read-ownership-tag column)))
  (declare (ignore iterator))
  (assert (<= (length content) cl-ds.common.rrb:+maximum-children-count+))
  (assert (not (zerop length)))
  (cl-ds.common.rrb:make-sparse-rrb-node
   :ownership-tag tag
   :content content
   :bitmask bitmask))


(declaim (inline truncate-mask))
(-> truncate-mask (integer) cl-ds.common.rrb:sparse-rrb-mask)
(defun truncate-mask (mask)
  (ldb (byte cl-ds.common.rrb:+maximum-children-count+ 0) mask))


(declaim (inline offset))
(defun offset (index)
  (declare (type integer index))
  (logandc2 index cl-ds.common.rrb:+tail-mask+))


(declaim (inline tree-index))
(defun tree-index (index)
  (declare (type fixnum index))
  (logand index cl-ds.common.rrb:+tail-mask+))


(declaim (notinline pad-stack))
(-> pad-stack (sparse-material-column-iterator
               fixnum fixnum fixnum iterator-stack
               sparse-material-column)
    t)
(defun pad-stack (iterator depth index new-depth stack column)
  (assert (> new-depth depth))
  (let ((depth-difference (- new-depth depth)))
    (iterate
      (for j from depth downto 0)
      (for k from new-depth downto 0)
      (shiftf (aref stack k)
              (aref stack j)
              nil))
    (iterate
      (with tag = (cl-ds.common.abstract:read-ownership-tag column))
      (for j from (1- depth-difference) downto 0)
      (for byte-position = (* (- new-depth j)
                              cl-ds.common.rrb:+bit-count+))
      (for i = (ldb (byte cl-ds.common.rrb:+bit-count+
                          byte-position)
                    index))
      (for prev-node = (aref stack (1+ j)))
      (for node = (if (null prev-node)
                      nil
                      (make-node
                       iterator column (ash 1 i)
                       :tag tag
                       :content (vector prev-node))))
      (setf (aref stack j) node)))
  (aref stack 0))


(declaim (notinline moove/pad-stack))
(defun move/pad-stack (iterator index new-index depth new-depth stack column)
  (when (> new-depth depth)
    (pad-stack iterator depth (max index 0) new-depth stack column))
  (move-stack (max depth new-depth) new-index stack))


(defun calculate-depth (index)
  (~> index
      integer-length
      (floor cl-ds.common.rrb:+bit-count+)))


(declaim (inline index-promoted))
(defun index-promoted (old-index new-index)
  (declare (type fixnum old-index new-index)
           (optimize (speed 3) (safety 0) (space 0)
                     (compilation-speed 0) (debug 0)))
  (not (eql (ceiling (the fixnum (1+ old-index))
                     cl-ds.common.rrb:+maximum-children-count+)
            (ceiling (the fixnum (1+ new-index))
                     cl-ds.common.rrb:+maximum-children-count+))))


(declaim (notinline fill-buffer))
(-> fill-buffer (fixnum iterator-buffer iterator-stack) t)
(defun fill-buffer (depth buffer stack)
  (declare (type fixnum depth)
           (type iterator-stack stack)
           (type iterator-buffer buffer))
  (let ((node (aref stack depth)))
    (declare (type (or null cl-ds.common.rrb:sparse-rrb-node-tagged) node))
    (when (null node)
      (map-into buffer (constantly :null))
      (return-from fill-buffer nil))
    (iterate
      (declare (type fixnum i))
      (for i from 0 below #.cl-ds.common.rrb:+maximum-children-count+)
      (for present = (cl-ds.common.rrb:sparse-rrb-node-contains node i))
      (setf (aref buffer i)
            (if present
                (cl-ds.common.rrb:sparse-nref node i)
                :null)))
    node))


(declaim (notinline move-column-to))
(defun move-column-to (iterator new-index column-index
                       depth
                       indexes
                       depths
                       stacks
                       columns
                       changes
                       buffers
                       initialization-status
                       &key
                         (force-initialization nil)
                         (promoted (index-promoted (aref indexes column-index)
                                                   new-index)))
  (declare (optimize (speed 3) (compilation-speed 0)
                     (space 0) (debug 0) (safety 0))
           (type fixnum column-index new-index)
           (type simple-vector stacks columns changes buffers initialization-status)
           (type (simple-array fixnum (*)) depths indexes))
  (let ((index (aref (read-indexes iterator) column-index)))
    (declare (type fixnum index)
             (type boolean promoted))
    (when (or (= new-index index)
              (nor promoted force-initialization))
      (setf (aref (read-indexes iterator) column-index) new-index)
      (return-from move-column-to nil))
    (when (nor (svref initialization-status column-index)
               force-initialization)
      (return-from move-column-to nil))
    (let ((new-depth (max (aref depths column-index)
                          depth))
          (not-changed (every #'null (aref changes column-index))))
      (unless not-changed
        (change-leaf iterator
                     (aref depths column-index)
                     (aref stacks column-index)
                     (aref columns column-index)
                     (aref changes column-index)
                     (aref buffers column-index))
        (let ((change (svref changes column-index)))
          (declare (type iterator-change change))
          (macrolet ((unrolled ()
                       `(progn
                          ,@(iterate
                              (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
                              (collecting `(setf (svref change ,i) nil))))))
            (unrolled)))
        (let ((buffer (svref buffers column-index)))
          (declare (type simple-vector buffer))
          (macrolet ((unrolled ()
                       `(progn
                          ,@(iterate
                              (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
                              (collecting `(setf (svref buffer ,i) :null))))))
            (unrolled)))
        (reduce-stack iterator index
                      (aref depths column-index)
                      (svref stacks column-index)
                      (svref columns column-index)))
      (if (and not-changed (not force-initialization))
          (setf (aref initialization-status column-index) nil)
          (progn
            (move/pad-stack iterator
                            (aref indexes column-index)
                            new-index
                            (aref depths column-index)
                            new-depth
                            (aref stacks column-index)
                            (aref columns column-index))
            (setf (aref indexes column-index) new-index
                  (aref depths column-index) new-depth
                  (aref initialization-status column-index) t)
            (fill-buffer new-depth
                         (aref buffers column-index)
                         (aref stacks column-index)))))
    nil))


(declaim (notinline initialize-iterator-column))
(-> initialize-iterator-column (sparse-material-column-iterator
                                fixnum t iterator-stack iterator-buffer
                                fixnum boolean fixnum)
    t)
(defun initialize-iterator-column (iterator index column stack buffer shift
                                   touched column-index)
  (declare (ignore buffer shift))
  (let ((indexes (read-indexes iterator))
        (depths (read-depths iterator))
        (stacks (read-stacks iterator))
        (columns (read-columns iterator))
        (changes (read-changes iterator))
        (buffers (read-buffers iterator))
        (initialization-status (read-initialization-status iterator)))
    (let ((column-root (column-root column)))
      (unless touched
        (ensure (aref stack 0) column-root)
        (loop :for i :from 1 :below (length stack)
              :do (setf (aref stack i) nil))))
    (move-column-to iterator index column-index
                    (calculate-depth index)
                    indexes
                    depths
                    stacks
                    columns
                    changes
                    buffers
                    initialization-status
                    :force-initialization t)))


(-> initialize-iterator-columns (sparse-material-column-iterator) t)
(defun initialize-iterator-columns (iterator)
  (iterate
    (declare (type fixnum i))
    (with index = (access-index iterator))
    (with stacks = (read-stacks iterator))
    (with columns = (read-columns iterator))
    (with buffers = (read-buffers iterator))
    (with depths = (read-depths iterator))
    (with touched = (read-touched iterator))
    (for i from 0 below (length stacks))
    (initialize-iterator-column iterator
                                index
                                (aref columns i)
                                (aref stacks i)
                                (aref buffers i)
                                (aref depths i)
                                (aref touched i)
                                i)))


(defun children (nodes)
  (declare (optimize (speed 3)))
  (lret ((result (make-hash-table)))
    (iterate
      (for (index n) in-hashtable nodes)
      (when (null n) (next-iteration))
      (iterate
        (declare (type fixnum i))
        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
        (when (cl-ds.common.rrb:sparse-rrb-node-contains n i)
          (setf (gethash (child-index index i) result)
                (cl-ds.common.rrb:sparse-nref n i)))))))


(defun max-index (nodes)
  (iterate outer
    (declare (type fixnum i length))
    (with length = (length nodes))
    (for i from 0 below length)
    (for column = (aref nodes i))
    (when (null column)
      (next-iteration))
    (iterate
      (for (index n) in-hashtable column)
      (when (null n)
        (next-iteration))
      (in outer (maximizing index into max-index)))
    (finally (return-from outer max-index))))


(defun find-max-index (nodes)
  (iterate outer
    (declare (type fixnum i length))
    (with length = (length nodes))
    (for i from 0 below length)
    (for column = (aref nodes i))
    (when (null column)
      (next-iteration))
    (iterate
      (for (index n) in-hashtable column)
      (when (null n)
        (next-iteration))
      (in outer (maximizing index into max-index)))
    (finally (return-from outer max-index))))


(defun gather-masks (nodes &optional (result (make-hash-table
                                              :size (* 32 (length nodes)))))
  (declare (type simple-array nodes))
  (iterate outer
    (declare (type fixnum i length))
    (with length = (length nodes))
    (for i from 0 below length)
    (for column = (aref nodes i))
    (when (null column)
      (next-iteration))
    (iterate
      (for (index n) in-hashtable column)
      (when (null n)
        (next-iteration))
      (in outer (maximizing index into max-index))
      (for existing-mask = (cl-ds.common.rrb:sparse-rrb-node-bitmask
                            (the cl-ds.common.rrb:sparse-rrb-node-tagged n)))
      (for mask = (gethash index result 0))
      (setf (gethash index result) (logior mask existing-mask)))
    (finally (return-from outer (values result max-index)))))


(defun clear-masks (state)
  (let ((table (concatenation-state-masks state)))
    (iterate
      (for (key value) in-hashtable table)
      (setf (gethash key table) 0)))
  state)


(defun concatenation-state-masks-logcount (state)
  (iterate
    (for (index mask) in-hashtable (concatenation-state-masks state))
    (sum (logcount mask))))


(defun concatenation-state-nodes-logcount (state)
  (iterate outer
    (for column in-vector (concatenation-state-nodes state))
    (iterate
      (for (index node) in-hashtable column)
      (when (null node)
        (next-iteration))
      (in outer (~> node
                    cl-ds.common.rrb:sparse-rrb-node-bitmask
                    logcount
                    sum)))))


(defun concatenation-state (iterator columns nodes parents)
  (bind ((max-index (find-max-index nodes))
         (result (make-concatenation-state
                  :changed-parents (map 'vector
                                        (lambda (x)
                                          (declare (ignore x))
                                          (make-hash-table))
                                        columns)
                  :iterator iterator
                  :max-index max-index
                  :nodes nodes
                  :columns columns
                  :parents parents)))
    result))


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


(-> node (concatenation-state t fixnum)
    (or null cl-ds.common.rrb:sparse-rrb-node-tagged))
(defun node (state column index)
  (declare (type concatenation-state state)
           (type fixnum index))
  (with-concatenation-state (state)
    (gethash index (aref nodes column))))


(-> (setf parent-changed) (boolean concatenation-state fixnum fixnum) boolean)
(defun (setf parent-changed) (new-value state column parent-index)
  (unless (null state)
    (with-concatenation-state (state)
      (if new-value
          (setf (gethash parent-index (aref changed-parents column)) t)
          (remhash parent-index (aref changed-parents column)))
      new-value)))


(-> (setf node) ((or null cl-ds.common.rrb:sparse-rrb-node-tagged)
                 concatenation-state fixnum fixnum)
    (or null cl-ds.common.rrb:sparse-rrb-node-tagged))
(defun (setf node) (new-value state column index)
  (declare (optimize (speed 3)))
  (assert (or (null new-value)
              (> (cl-ds.common.rrb:sparse-rrb-node-size new-value) 0)))
  (unless (null (concatenation-state-parents state))
    (unless (eq new-value (node state column index))
      (setf (parent-changed state column (parent-index index)) t)))
  (with-concatenation-state (state)
    (if (null new-value)
        (remhash index (aref nodes column))
        (progn
          (setf (gethash index (aref nodes column)) new-value)))
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


(defun logior-mask (state index mask)
  (with-concatenation-state (state)
    (setf #1=(gethash index masks 0) (logior mask #1#))))


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


(-> distinct-missing (fixnum fixnum) fixnum)
(defun distinct-missing (real-mask logior-mask)
  (~> real-mask
      (logxor logior-mask)
      truncate-mask))


(-> move-to-existing-column (concatenation-state fixnum fixnum
                                                 cl-ds.common.rrb:sparse-rrb-mask cl-ds.common.rrb:sparse-rrb-mask
                                                 fixnum)
    t)
(defun move-to-existing-column (state from to from-mask to-mask column-index)
  (assert (< to from))
  (assert (= (logcount to-mask) (integer-length to-mask)))
  (assert (= (logcount from-mask) (integer-length from-mask)))
  (with-concatenation-state (state)
    (let* ((column (aref columns column-index))
           (column-tag (cl-ds.common.abstract:read-ownership-tag column))
           (from-node (node state column-index from))
           (to-node (node state column-index to))
           (taken (logcount to-mask))
           (free-space (- cl-ds.common.rrb:+maximum-children-count+
                          taken))
           (from-content (cl-ds.common.rrb:sparse-rrb-node-content from-node))
           (real-from-size (cl-ds.common.rrb:sparse-rrb-node-size from-node))
           (element-type (array-element-type from-content))
           (from-owned (cl-ds.common.abstract:acquire-ownership
                        from-node column-tag))
           (to-owned (cl-ds.common.abstract:acquire-ownership
                      to-node column-tag))
           (to-content (cl-ds.common.rrb:sparse-rrb-node-content to-node))
           (real-to-size (cl-ds.common.rrb:sparse-rrb-node-size to-node))
           (real-to-mask (cl-ds.common.rrb:sparse-rrb-node-bitmask to-node))
           (real-from-mask (cl-ds.common.rrb:sparse-rrb-node-bitmask from-node))
           (shifted-from-mask (truncate-mask (ash real-from-mask taken)))
           (shifted-count (logcount shifted-from-mask))
           (new-from-mask (ldb (byte cl-ds.common.rrb:+maximum-children-count+ free-space)
                               real-from-mask))
           (new-to-mask (logior real-to-mask shifted-from-mask))
           (new-to-size (logcount new-to-mask)))
      (declare (type (simple-array * (*)) from-content)
               (type cl-ds.common.rrb:sparse-rrb-mask new-from-mask new-to-mask real-from-mask real-to-mask)
               (type fixnum taken free-space real-from-size new-to-size))
      (assert (= real-to-mask (logand real-to-mask to-mask)))
      (assert (= real-from-mask (logand real-from-mask from-mask)))
      (assert (zerop (logand to-mask shifted-from-mask)))
      (assert (zerop (logand real-to-mask shifted-from-mask)))
      (assert (< new-from-mask real-from-mask))
      (assert (<= (logcount new-from-mask) (logcount from-mask)))
      (assert (= (+ (logcount new-from-mask)
                    (logcount new-to-mask))
                 (+ (logcount real-from-mask)
                    (logcount real-to-mask))))
      (if (and to-owned
               (>= (length to-content) new-to-size))
          (iterate
            (declare (type fixnum i j shifted-count))
            (for j from 0 below real-from-size)
            (for i from real-to-size below (length to-content))
            (repeat shifted-count)
            (setf (aref to-content i) (aref from-content j))
            (finally (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask to-node)
                           new-to-mask)))
          (let* ((new-content-size
                   (if (zerop new-from-mask)
                       (~>> (ash from-mask taken)
                            (logior to-mask)
                            truncate-mask
                            (distinct-missing new-to-mask)
                            logcount
                            (- cl-ds.common.rrb:+maximum-children-count+))
                       new-to-size))
                 (new-content (if (eq element-type t)
                                  (make-array
                                   new-content-size
                                   :initial-element cl-ds.meta:null-bucket
                                   :element-type element-type)
                                  (make-array
                                   new-content-size
                                   :element-type element-type))))
            (declare (type (simple-array * (*)) new-content)
                     (type fixnum new-content-size))
            (assert (>= new-content-size new-to-size))
            (assert (>= new-to-size real-to-size))
            (iterate
              (declare (type fixnum i))
              (for i from 0 below real-to-size)
              (setf (aref new-content i) (aref to-content i)))
            (iterate
              (declare (type fixnum i j))
              (for i from real-to-size below new-to-size)
              (for j from 0)
              (setf (aref new-content i) (aref from-content j)))
            (if to-owned
                (setf (cl-ds.common.rrb:sparse-rrb-node-content to-node) new-content
                      (cl-ds.common.rrb:sparse-rrb-node-bitmask to-node) new-to-mask)
                (setf (node state column-index to)
                      (make-node iterator column new-to-mask
                                 :content new-content)))))
      (cond ((zerop new-from-mask)
             (setf (node state column-index from) nil))
            (from-owned
             (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask from-node)
                   new-from-mask)
             (iterate
               (declare (type fixnum i j new-from-size))
               (with new-from-size = (logcount new-from-mask))
               (for i from free-space below real-from-size)
               (for j from 0 below new-from-size)
               (setf (aref from-content j) (aref from-content i))))
            (t (let* ((distinct-missing (ldb (byte cl-ds.common.rrb:+maximum-children-count+
                                                   free-space)
                                             (distinct-missing real-from-mask
                                                               from-mask)))
                      (desired-size (- cl-ds.common.rrb:+maximum-children-count+
                                       (logcount distinct-missing)))
                      (new-from (make-node iterator
                                           column new-from-mask
                                           :length desired-size
                                           :type element-type))
                      (new-content (cl-ds.common.rrb:sparse-rrb-node-content
                                    new-from)))
                 (declare (type (simple-array * (*)) new-content))
                 (iterate
                   (declare (type fixnum i j))
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
  (declare (optimize (speed 3)))
  (with-concatenation-state (state)
    (bind ((column (aref columns column-index))
           (column-tag (cl-ds.common.abstract:read-ownership-tag column))
           (from-node (node state column-index from))
           (to-node (node state column-index to))
           (to-exists (not (null to-node)))
           (from-exists (not (null from-node)))
           (from-owned (cl-ds.common.abstract:acquire-ownership
                        from-node column-tag)))
      (unless from-exists
        (return-from move-children-in-column nil))
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
                              (unsigned-byte 32) (unsigned-byte 32)
                              cl-ds.common.rrb:sparse-rrb-mask
                              cl-ds.common.rrb:sparse-rrb-mask)
    t)
(defun move-children-in-columns (state from to from-mask to-mask)
  (iterate
    (with columns = (concatenation-state-columns state))
    (for i from 0 below (length columns))
    (move-children-in-column state from to from-mask to-mask i)))


(defun move-children (state from to)
  (declare (optimize (speed 3)))
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
                 (move-children-in-columns state from to from-mask to-mask)
                 0)
                (t
                 (move-children-in-columns state from to from-mask to-mask)
                 1))
        (setf (mask state from) (ldb (byte cl-ds.common.rrb:+maximum-children-count+
                                           free-space)
                                     from-mask)
              (mask state to) (~> (ash from-mask taken)
                                  (logior to-mask)
                                  truncate-mask))))))


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


(defun clear-changed-parents-masks (state)
  (with-concatenation-state (state)
    (iterate
      (declare (type fixnum i))
      (for i from 0 below (length nodes))
      (iterate
        (for (index changed) in-hashtable (aref changed-parents i))
        (assert changed)
        (setf (mask parents index) 0)))))


(defun update-parents (state column)
  (declare (type concatenation-state state))
  (with-concatenation-state (state)
    (iterate
      (declare (type cl-ds.common.rrb:sparse-rrb-mask mask))
      (with column-object = (aref columns column))
      (with tag = (cl-ds.common.abstract:read-ownership-tag column-object))
      (for (index changed) in-hashtable (aref changed-parents column))
      (assert changed)
      (for parent = (node parents column index))
      (for mask = 0)
      (iterate
        (declare (type fixnum i child-index))
        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
        (for child-index = (child-index index i))
        (for child = (node state column child-index))
        (assert (or (null child)
                    (> (cl-ds.common.rrb:sparse-rrb-node-size child) 0)))
        (unless (null child)
          (setf mask (dpb 1 (byte 1 i) mask))))
      (setf mask (truncate-mask mask))
      (if (zerop mask)
          (setf (node parents column index) nil)
          (let ((new-content (make-array (logcount mask)
                                         :element-type t
                                         :initial-element cl-ds.meta:null-bucket)))
            (iterate
              (declare (type fixnum i content-position))
              (with content-position = 0)
              (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
              (unless (ldb-test (byte 1 i) mask)
                (next-iteration))
              (for child-index = (child-index index i))
              (for child = (node state column child-index))
              (assert child)
              (setf (aref new-content content-position) child)
              (incf content-position))
            (if (and parent (cl-ds.common.abstract:acquire-ownership parent tag))
                (progn
                  (assert new-content)
                  (setf (cl-ds.common.rrb:sparse-rrb-node-content parent)
                        new-content
                        (cl-ds.common.rrb:sparse-rrb-node-bitmask parent)
                        mask))
                (let ((new-node (make-node iterator column-object mask
                                           :content new-content)))
                  (setf (node parents column index) new-node))))))))


(defun concatenate-trees (iterator
                          &aux (columns
                                (~>> iterator read-columns
                                     (remove-if #'null _ :key #'column-root))))
  (declare (type simple-vector columns))
  (when (emptyp columns)
    (return-from concatenate-trees nil))
  (bind ((depth (the fixnum
                     (~> columns
                         (extremum #'> :key #'cl-ds.dicts.srrb:access-shift)
                         cl-ds.dicts.srrb:access-shift)))
         ((:labels impl (d nodes parent-state))
          (declare (type simple-vector nodes))
          (let* ((current-state (concatenation-state iterator
                                                     columns
                                                     nodes
                                                     parent-state)))
            (concatenate-masks current-state)
            (unless (eql d depth)
              (impl (the fixnum (1+ d))
                    (map 'vector #'children nodes)
                    current-state))
            (concatenate-masks current-state)
            (shift-content current-state)
            (unless (null parent-state)
              (iterate
                (declare (type fixnum i))
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


(defun build-new-mask (old-bitmask missing-mask)
  (declare (type cl-ds.common.rrb:sparse-rrb-mask old-bitmask missing-mask))
  (let* ((distinct-missing (~> old-bitmask
                               lognot
                               truncate-mask
                               (logxor missing-mask)))
         (new-bitmask (ldb (byte (logcount (logior old-bitmask distinct-missing)) 0)
                           #.(iterate
                               (with result = 0)
                               (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
                               (setf (ldb (byte 1 i) result) 1)
                               (finally (return result))))))
    (declare (type cl-ds.common.rrb:sparse-rrb-mask new-bitmask distinct-missing))
    (assert (zerop (logand distinct-missing missing-mask)))
    (iterate
      (declare (type fixnum i zero-sum)
               (type (integer 0 32) new-index old-index))
      (with zero-sum = 0)
      (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
      (unless (ldb-test (byte 1 i) distinct-missing)
        (next-iteration))
      (for old-index = (~> (ldb (byte i 0) old-bitmask)
                           logcount))
      (for new-index = (+ old-index zero-sum))
      (setf new-bitmask (the cl-ds.common.rrb:sparse-rrb-mask (dpb 0 (byte 1 new-index) new-bitmask)))
      (the fixnum (incf zero-sum))
      (finally (return new-bitmask)))))


(-> concatenate-masks (concatenation-state) t)
(defun concatenate-masks (state)
  (with-concatenation-state (state)
    (clrhash masks)
    (gather-masks nodes masks)
    (iterate
      (declare (type fixnum i))
      (for i from 0 below (length columns))
      (for tree = (aref nodes i))
      (for column = (aref columns i))
      (for tag = (cl-ds.common.abstract:read-ownership-tag column))
      (maphash
       (lambda (index node)
         (let* ((mask (mask state index))
                (old-mask (cl-ds.common.rrb:sparse-rrb-node-bitmask node))
                (new-mask (~>> mask
                               lognot
                               truncate-mask
                               (build-new-mask old-mask)))
                (owned (cl-ds.common.abstract:acquire-ownership node tag)))
           (declare (type cl-ds.common.rrb:sparse-rrb-mask old-mask new-mask))
           (unless (= old-mask new-mask)
             (if owned
                 (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask node) new-mask)
                 (let ((copy (cl-ds.common.rrb:deep-copy-sparse-rrb-node node 0 tag)))
                   (setf (cl-ds.common.rrb:sparse-rrb-node-bitmask copy) new-mask
                         (node state i index) copy))))))
       tree))))


(defun move-stack (depth new-index stack &aux (node (aref stack 0)))
  (declare (type fixnum depth new-index)
           (type iterator-stack stack))
  (assert (>= new-index 0))
  (assert (typep node '(or null cl-ds.common.rrb:sparse-rrb-node-tagged)))
  (when (null node)
    (return-from move-stack nil))
  (iterate outer
    (declare (type fixnum i offset byte-position)
             (type boolean present))
    (for i from 1 to depth)
    (for byte-position
         from (* cl-ds.common.rrb:+bit-count+
                 depth)
         downto 0
         by cl-ds.common.rrb:+bit-count+)
    (for offset = (ldb (byte cl-ds.common.rrb:+bit-count+ byte-position)
                       new-index))
    (for present = (cl-ds.common.rrb:sparse-rrb-node-contains node offset))
    (unless present
      (iterate
        (declare (type fixnum j))
        (for j from i to depth)
        (setf (aref stack j) nil))
      (leave))
    (setf node (the cl-ds.common.rrb:sparse-rrb-node-tagged
                    (cl-ds.common.rrb:sparse-nref node offset))
          (aref stack i) node))
  (aref stack 0))


(declaim (notinline mutate-leaf))
(defun mutate-leaf (column old-node change buffer
                    &optional (new-size (- cl-ds.common.rrb:+maximum-children-count+
                                           (count :null buffer))))
  (declare (type cl-ds.common.rrb:sparse-rrb-node-tagged old-node))
  (let* ((old-content (cl-ds.common.rrb:sparse-rrb-node-content old-node))
         (old-size (array-dimension old-content 0))
         (bitmask 0)
         (new-content (if (>= old-size new-size)
                          old-content
                          (make-array new-size
                                      :element-type (column-type column)))))
    (declare (type (or simple-vector simple-bit-vector) old-content)
             (type fixnum old-size new-size)
             (type cl-ds.common.rrb:sparse-rrb-mask bitmask))
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


(declaim (notinline make-leaf))
(defun make-leaf (iterator column old-node change buffer
                  &optional (new-size (- cl-ds.common.rrb:+maximum-children-count+
                                         (count :null buffer))))
  (declare (type simple-vector buffer change))
  (unless (null old-node)
    (iterate
      (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
      (collect (let ((changed (svref change i)))
                  (unless (or changed
                              (not (cl-ds.common.rrb:sparse-rrb-node-contains old-node
                                                                              i)))
                    (setf (svref buffer i) (cl-ds.common.rrb:sparse-nref old-node i)))))))
  (let ((new-content (make-array new-size :element-type (column-type column)))
        (bitmask 0))
    (declare (type cl-ds.common.rrb:sparse-rrb-mask bitmask)
             (type (simple-array * (*)) new-content))
    (macrolet ((unrolled ()
                 `(let ((index 0))
                    (declare (type fixnum index))
                    ,@(iterate
                        (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
                        (collect `(let ((v (aref buffer ,i)))
                                    (unless (eql v :null)
                                      (setf (aref new-content index) v
                                            bitmask (dpb 1 (byte 1 ,i) bitmask)
                                            index (the fixnum (1+ index))))))))))
      (unrolled))
    (make-node iterator column bitmask :content new-content)))


(defun change-leaf (iterator depth stack column change buffer)
  (declare (optimize (speed 3) (safety 0) (space 0)
                     (debug 0) (compilation-speed 0)))
  (declare (type iterator-buffer buffer)
           (type fixnum depth)
           (type iterator-stack stack))
  (macrolet ((unrolled ()
               `(+ ,@(iterate
                       (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
                       (collecting `(if (eq :null (svref buffer ,i))
                                        0
                                        1))))))
    (let ((new-size (the fixnum (unrolled))))
      (cond ((zerop new-size)
             (setf (aref stack depth) nil))
            (t
             (let* ((tag (cl-ds.common.abstract:read-ownership-tag column))
                    (old-node (aref stack depth)))
               (if (or (null old-node)
                       (not (cl-ds.common.abstract:acquire-ownership old-node
                                                                     tag)))
                   (unless (zerop new-size)
                     (setf (aref stack depth)
                           (make-leaf iterator column old-node change buffer new-size)))
                   (mutate-leaf column old-node change buffer new-size)))))))
  nil)


(defun change-leafs (iterator)
  (declare (optimize (speed 3) (safety 0) (space 0)))
  (let* ((initialization-status (read-initialization-status iterator))
         (depths (read-depths iterator))
         (stacks (read-stacks iterator))
         (columns (read-columns iterator))
         (changes (read-changes iterator))
         (buffers (read-buffers iterator))
         (length (length depths)))
    (declare (type simple-vector stacks columns changes)
             (type (or simple-vector simple-bit-vector) buffers)
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
  (assert (typep parent '(or null cl-ds.common.rrb:sparse-rrb-node-tagged)))
  (assert (typep child '(or null cl-ds.common.rrb:sparse-rrb-node-tagged)))
  (flet ((empty-node (node)
           (or (null node)
               (zerop (cl-ds.common.rrb:sparse-rrb-node-size node)))))
    (cond ((and (empty-node parent) (empty-node child))
           nil)
          ((null parent)
           (make-node iterator column (ash 1 position)
                      :content (vector child)))
          ((and (empty-node child)
                (eql 1 (cl-ds.common.rrb:sparse-rrb-node-size parent)))
           (if (cl-ds.common.rrb:sparse-rrb-node-contains parent position)
               nil
               parent))
          ((cl-ds.common.abstract:acquire-ownership parent tag)
           (if (empty-node child)
               (when (cl-ds.common.rrb:sparse-rrb-node-contains parent position)
                 (cl-ds.common.rrb:sparse-rrb-node-erase! parent position))
               (setf (cl-ds.common.rrb:sparse-nref parent position)
                     child))
           parent)
          (t (lret ((copy (cl-ds.common.rrb:deep-copy-sparse-rrb-node
                           parent
                           0
                           tag)))
               (if (empty-node child)
                   (when (cl-ds.common.rrb:sparse-rrb-node-contains copy position)
                     (cl-ds.common.rrb:sparse-rrb-node-erase! copy position))
                   (setf (cl-ds.common.rrb:sparse-nref copy position)
                         child)))))))


(declaim (notinline reduce-stack))
(-> reduce-stack (sparse-material-column-iterator fixnum fixnum iterator-stack sparse-material-column) t)
(defun reduce-stack (iterator index depth stack column)
  (declare (optimize (speed 3) (safety 0)))
  (let ((prev-node (aref stack depth)))
    (iterate
      (declare (type fixnum i bits))
      (with tag = (cl-ds.common.abstract:read-ownership-tag column))
      (for i from (1- depth) downto 0)
      (for bits
           from cl-ds.common.rrb:+bit-count+
           by cl-ds.common.rrb:+bit-count+)
      (for node = (aref stack i))
      (for position = (ldb (byte cl-ds.common.rrb:+bit-count+ bits)
                           index))
      (for new-node = (copy-on-write-node iterator node prev-node
                                          position tag column))
      (assert (or (null new-node)
                  (> (cl-ds.common.rrb:sparse-rrb-node-size new-node) 0)))
      (until (eq node new-node))
      (setf prev-node new-node
            (aref stack i) new-node)))
  (aref stack 0))


(declaim (notinline fill-buffers))
(-> fill-buffers (sparse-material-column-iterator) t)
(defun fill-buffers (iterator)
  (let ((depths (read-depths iterator))
        (initialization-status (read-initialization-status iterator))
        (buffers (read-buffers iterator))
        (stacks (read-stacks iterator)))
    (iterate
      (declare (type fixnum i))
      (for i from 0 below (length stacks))
      (when (aref initialization-status i)
        (fill-buffer (aref depths i) (aref buffers i) (aref stacks i))))))


(declaim (notinline reduce-stacks))
(-> reduce-stacks (sparse-material-column-iterator) t)
(defun reduce-stacks (iterator)
  (let ((initialization-status (read-initialization-status iterator))
        (depths (read-depths iterator))
        (stacks (read-stacks iterator))
        (index (access-index iterator))
        (columns (read-columns iterator)))
    (iterate
      (declare (type fixnum i))
      (for i from 0 below (length stacks))
      (when (aref initialization-status i)
        (reduce-stack iterator index
                      (aref depths i)
                      (aref stacks i)
                      (aref columns i))))))


(declaim (notinline clear-changes))
(-> clear-changes (sparse-material-column-iterator) t)
(defun clear-changes (iterator)
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
      (iterate
        (declare (type iterator-buffer ch)
                 (type fixnum i))
        (with ch = change)
        (for i from 0 below (length ch))
        (setf (aref ch i) nil)))))


(declaim (notinline clear-buffers))
(-> clear-buffers (sparse-material-column-iterator) t)
(defun clear-buffers (iterator)
  (iterate
    (declare (type fixnum length i))
    (with buffers = (read-buffers iterator))
    (with initialization-status = (read-initialization-status iterator))
    (with length = (length buffers))
    (for i from 0 below length)
    (for buffer = (aref buffers i))
    (for initialized = (aref initialization-status i))
    (when initialized
      (iterate
        (declare (type iterator-buffer bu)
                 (type fixnum i))
        (with bu = buffer)
        (for i from 0 below (length bu))
        (setf (aref bu i) :null)))))


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
  (bind ((shift (cl-ds.dicts.srrb:access-shift column))
         ((:labels skip (node &optional (s shift)))
          (if (or (zerop s)
                  (~> node
                      cl-ds.common.rrb:sparse-rrb-node-bitmask
                      (eql 1)
                      not))
              node
              (skip (cl-ds.common.rrb:sparse-nref node 0)
                    (1- s))))
         (tree-index-bound (cl-ds.dicts.srrb:scan-index-bound column))
         (index-bound (* #.cl-ds.common.rrb:+maximum-children-count+
                         (1+ (ceiling tree-index-bound #.cl-ds.common.rrb:+maximum-children-count+))))
         (root (cl-ds.dicts.srrb:access-tree column)))
    (setf (cl-ds.dicts.srrb:access-tree-index-bound column) tree-index-bound
          (cl-ds.dicts.srrb:access-index-bound column) index-bound

          (cl-ds.dicts.srrb:access-shift column)
          (cl-ds.dicts.srrb:shift-for-position (1- tree-index-bound))

          (cl-ds.dicts.srrb:access-tree column) (if (cl-ds.meta:null-bucket-p root)
                                                    root
                                                    (skip root)))))


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
