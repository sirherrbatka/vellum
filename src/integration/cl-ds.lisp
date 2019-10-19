(in-package #:cl-df.int)


(defun obtain-depth (range &optional (depth 1))
  (typecase range
    (cl-ds.alg:group-by-result-range
     (obtain-depth (~> range cl-ds:peek-front cdr)
                   (1+ depth)))
    (t depth)))


(defmethod cl-df:to-table ((range cl-ds.alg:group-by-result-range)
                           &key
                             (key #'identity)
                             (header-class 'cl-df:standard-header)
                             (class 'cl-df.table:standard-table)
                             (columns '()))
  (bind ((column-definitions columns)
         (group-by-depth (~> range cl-ds:peek-front cdr obtain-depth))
         (column-count (1+ group-by-depth))
         (columns (make-array column-count))
         (columns-buffer (make-array column-count))
         (header (apply #'cl-df:make-header
                        header-class
                        (iterate
                          (for i from 0 below column-count)
                          (for column = (first column-definitions))
                          (pop column-definitions)
                          (collect column))))
         (iterator nil)
         ((:labels impl (element position))
          (if (typep element 'cl-ds.alg:group-by-result-range)
              (cl-ds:across element
                            (lambda (group)
                              (setf (aref columns-buffer position)
                                    (car group))
                              (impl (cdr group) (1+ position))))
              (progn
                (setf (aref columns-buffer (1- column-count))
                      (funcall key element))
                (iterate
                  (for i from 0 below column-count)
                  (setf (cl-df.column:iterator-at iterator i)
                        (aref columns-buffer i))
                  (finally (cl-df.column:move-iterator iterator 1)))))))
    (iterate
      (for i from 0 below column-count)
      (setf (aref columns i)
            (cl-df.column:make-sparse-material-column
             :element-type t)))
    (setf iterator (cl-df.column:make-iterator columns))
    (impl range 0)
    (cl-df.column:finish-iterator iterator)
    (make class
          :header header
          :columns columns)))
