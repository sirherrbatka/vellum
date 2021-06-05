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
                                        'vellum.header:standard-header
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
               (iota (vellum.table:column-count vellum.table:*table*))
               (~> (apply #'vellum.selection:s forms)
                   (vellum.selection:address-range
                    (lambda (x) (vellum.header:ensure-index header x))
                    (vellum.header:column-count header))
                   cl-ds.alg:to-list))))
    (lambda (&rest ignored)
      (declare (ignore ignored))
      (mapcar #'vellum.header:rr selection))))


(defun row-to-vector (&rest forms)
  (let* ((header (vellum.header:header))
         (selection
           (if (endp forms)
               (iota (vellum.table:column-count vellum.table:*table*))
               (~> (apply #'vellum.selection:s forms)
                   (vellum.selection:address-range
                    (lambda (x) (vellum.header:ensure-index header x))
                    (vellum.header:column-count header))
                   cl-ds.alg:to-list))))
    (lambda (&rest ignored)
      (declare (ignore ignored))
      (map 'vector #'vellum.header:rr selection))))


(defun column-names (table)
  (~> table vellum.table:header vellum.header:column-names))
