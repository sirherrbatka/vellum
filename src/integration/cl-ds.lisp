(in-package #:cl-df.int)


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


(defmethod cl-df:to-table ((range cl-ds.alg:group-by-result-range)
                           &key
                             (key #'identity)
                             (header-class 'cl-df:standard-header)
                             (class 'cl-df.table:standard-table)
                             (columns '())
                             (header (apply #'cl-df:make-header
                                            header-class
                                            (gather-column-data range columns '()))))
  (bind ((column-count (cl-df.header:column-count header))
         (columns (make-array column-count))
         (columns-buffer (make-array column-count))
         (iterator nil))
    (iterate
      (for i from 0 below column-count)
      (setf (aref columns i)
            (cl-df.column:make-sparse-material-column
             :element-type (cl-df.header:column-type header i))))
    (setf iterator (cl-df.column:make-iterator columns))
    (fill-columns-buffer-impl
     range 0 columns-buffer
     (lambda ()
       (iterate
         (for i from 0 below column-count)
         (setf (cl-df.column:iterator-at iterator i)
               (aref columns-buffer i))
         (finally (cl-df.column:move-iterator iterator 1))))
     key)
    (cl-df.column:finish-iterator iterator)
    (make class
          :header header
          :columns columns)))
