(cl:in-package #:vellum.int)


(defgeneric gather-column-data (range definitions result))

(defgeneric fill-columns-buffer-impl (range position buffer
                                      finish-callback key))


(defmethod fill-columns-buffer-impl ((range cl-ds.alg:group-by-result-range)
                                     position buffer finish-callback key)
  (cl-ds:across range
                (lambda (group)
                  (setf (aref buffer position)
                        (car group))
                  (fill-columns-buffer-impl (cdr group) (1+ position)
                                            buffer finish-callback key))))


(defmethod fill-columns-buffer-impl ((range cl-ds.alg:summary-result-range)
                                     position buffer finish-callback key)
  (cl-ds:across range
                (lambda (group)
                  (setf (aref buffer position)
                        (funcall key (cdr group)))
                  (incf position)))
  (funcall finish-callback))


(defmethod fill-columns-buffer-impl ((range t) position buffer
                                     finish-callback key)
  (setf (aref buffer position) (funcall key range))
  (funcall finish-callback))


(defmethod gather-column-data ((range cl-ds.alg:group-by-result-range)
                               definitions result)
  (gather-column-data (~> range cl-ds:peek-front cdr)
                      (rest definitions)
                      (cons (first definitions) result)))


(defmethod gather-column-data ((range cl-ds.alg:summary-result-range)
                               definitions result)
  (cl-ds:across range
                (lambda (data)
                  (push (append (pop definitions)
                                (list :alias (car data)))
                        result)))
  (nreverse result))


(defmethod gather-column-data ((range t)
                               definitions result)
  (nreverse (cons (first definitions) result)))


(defun common-to-table (range key class header body)
  (bind ((column-count (vellum.header:column-count header))
         (columns (make-array column-count))
         (columns-buffer (make-array column-count)))
    (iterate
      (for i from 0 below column-count)
      (setf (aref columns i)
            (vellum.column:make-sparse-material-column
             :element-type (vellum.header:column-type header i))))
    (let* ((iterator (vellum.column:make-iterator columns))
           (vellum.header:*row* (make 'vellum.table:setfable-table-row
                                     :iterator iterator)))
      (fill-columns-buffer-impl
       range 0 columns-buffer
       (lambda ()
         (iterate
           (for i from 0 below column-count)
           (setf (vellum.column:iterator-at iterator i)
                 (aref columns-buffer i))
           (unless (null body)
             (funcall body vellum.header:*row*))
           (finally (vellum.column:move-iterator iterator 1))))
       key)
      (vellum.column:finish-iterator iterator)
      (make class
            :header header
            :columns columns))))


(defmethod vellum:to-table ((range cl-ds.alg:group-by-result-range)
                           &key
                             (key #'identity)
                             (header-class 'vellum:standard-header)
                             (class 'vellum.table:standard-table)
                             (columns '())
                             (body nil)
                             (header (apply #'vellum:make-header
                                            header-class
                                            (gather-column-data range columns '()))))
  (common-to-table range key class header body))


(defmethod vellum:to-table ((range cl-ds.alg:summary-result-range)
                           &key
                             (key #'identity)
                             (header-class 'vellum:standard-header)
                             (class 'vellum.table:standard-table)
                             (columns '())
                             (body nil)
                             (header (apply #'vellum:make-header
                                            header-class
                                            (gather-column-data range columns '()))))
  (common-to-table range key class header body))
