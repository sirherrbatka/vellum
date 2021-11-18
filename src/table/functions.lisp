(cl:in-package #:vellum.table)


(defun finish-transformation ()
  (funcall *transform-control* :finish))


(defun nullify ()
  (funcall *transform-control* :nullify))


(defun drop-row ()
  (funcall *transform-control* :drop))


(defun make-table (&key
                     (class 'standard-table)
                     (columns '() columns-p)
                     (header (if columns-p
                                 (apply #'vellum.header:make-header
                                        columns)
                                 (vellum.header:header))))
  (make-table* class header))


(defun hstack (frames &key (isolate t))
  (let ((list (if (listp frames)
                  frames
                  (cl-ds.alg:to-list frames))))
    (hstack* (first list) (rest list)
             :isolate isolate)))


(defun vstack (frames)
  (let ((list (if (listp frames)
                  frames
                  (cl-ds.alg:to-list frames))))
    (vstack* (first list) (rest list))))


(defun row-to-list (&rest forms)
  (let* ((header (vellum.header:header))
         (selection
           (if (endp forms)
               (iota (vellum.header:column-count header))
               (~> (apply #'vellum.selection:s forms)
                   (vellum.selection:address-range
                    (lambda (x) (vellum.header:ensure-index header x))
                    (vellum.header:column-count header))
                   cl-ds.alg:to-list))))
    (lambda (&rest ignored)
      (declare (ignore ignored))
      (mapcar #'rr selection))))


(defun row-to-vector (&rest forms)
  (let* ((header (vellum.header:header))
         (selection
           (if (endp forms)
               (iota (vellum.header:column-count header))
               (~> (apply #'vellum.selection:s forms)
                   (vellum.selection:address-range
                    (lambda (x) (vellum.header:ensure-index header x))
                    (vellum.header:column-count header))
                   cl-ds.alg:to-list))))
    (lambda (&rest ignored)
      (declare (ignore ignored))
      (map 'vector #'rr selection))))


(defun column-names (table)
  (~> table vellum.table:header vellum.header:column-names))


(declaim (inline rr))
(defun rr (index
           &optional (row (vellum.header:row)) (header (vellum.header:header)))
  (row-at header row index))


(declaim (inline (setf rr)))
(defun (setf rr) (new-value index
                  &optional (row (vellum.header:row)) (header (vellum.header:header)))
  (setf (row-at header row index) new-value))


(defun current-row-as-vector (&optional
                                (header (vellum.header:header))
                                (row (vellum.header:row)))
  (iterate
    (with column-count = (column-count header))
    (with result = (make-array column-count))
    (for i from 0 below column-count)
    (setf (aref result i) (rr i row))
    (finally (return result))))


(defun vs (&rest forms)
  (let ((header (vellum.header:header)))
    (~> (apply #'vellum.selection:s forms)
        (vellum.selection:address-range
         (lambda (x) (vellum.header:ensure-index header x))
         (vellum.header:column-count header))
        (cl-ds.alg:on-each (lambda (x) (rr x))))))


(defun make-bind-row (optimized-closure non-optimized-closure)
  (lret ((result (make 'bind-row :optimized-closure optimized-closure)))
    (c2mop:set-funcallable-instance-function result non-optimized-closure)))


(declaim (inline row-at))
(defun row-at (header row name)
  (let ((column (if (integerp name)
                    name
                    (vellum.header:name-to-index header name))))
    (declare (type integer column))
    (etypecase row
      (table-row
       (~> row table-row-iterator (vellum.column:iterator-at column)))
      (simple-vector
       (let ((length (length row)))
         (declare (type fixnum length))
         (unless (< -1 column length)
           (error 'no-column
                  :bounds `(0 ,length)
                  :argument 'column
                  :value column
                  :format-arguments (list column)))
         (locally (declare (optimize (speed 3) (safety 0)
                                     (space 0) (debug 0)))
           (aref row column))))
      (sequence
       (let ((length (length row)))
         (unless (< -1 column length)
           (error 'no-column
                   :bounds (iota length)
                   :argument 'column
                   :value column
                   :format-arguments (list column)))
         (elt row column))))))


(declaim (inline (setf row-at)))
(defun (setf row-at) (new-value header row name)
  (let ((column (if (integerp name)
                    name
                    (vellum.header:name-to-index header name))))
    (declare (type integer column))
    (etypecase row
      (setfable-table-row
       (setf (~> row setfable-table-row-iterator (vellum.column:iterator-at column))
             new-value))
      (simple-vector
        (let ((length (length row)))
          (declare (type fixnum length))
          (unless (< -1 column length)
            (error 'no-column
                   :bounds `(0 ,length)
                   :argument 'column
                   :value column
                   :format-arguments (list column)))
          (locally (declare (optimize (speed 3) (safety 0)
                                      (space 0) (debug 0)))
            (setf (aref row column) new-value))))
      (sequence
       (let ((length (length row)))
         (unless (< -1 column length)
           (error 'no-column
                   :bounds (iota length)
                   :argument 'column
                   :value column
                   :format-arguments (list column)))
         (setf (elt row column) new-value))))))
