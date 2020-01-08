(cl:in-package #:cl-df.int)


(defclass postgres-query (cl-ds:traversable)
  ((%query :initarg :query
           :type list
           :reader read-query)))


(defmethod cl-ds:traverse ((object postgres-query) function)
  (bind ((header (cl-df.header:header))
         (column-count (cl-df.header:column-count header))
         (row (make-array column-count :initial-element :null))
         ((:slots %query) object)
         (query (s-sql:sql-compile %query)))
    (cl-df.header:set-row row)
    (cl-postgres:exec-query
     postmodern:*database* query
     (cl-postgres:row-reader (fields)
       (unless (= column-count (length fields))
         (error "Number of columns in the header does not match number of selected fields in query."))
       (iterate
         (while (cl-postgres:next-row))
         (iterate
           (for i from 0 below column-count)
           (setf (aref row i) (cl-postgres:next-field (elt fields i))))
         (funcall function row))))
    object))


(defmethod cl-ds:across ((object postgres-query) function)
  (cl-ds:traverse object function))


(defmethod cl-ds:reset! ((object postgres-query))
  object)


(defmethod cl-ds:clone ((object postgres-query))
  (make 'postgres-query
        :query (read-query object)))


(defmethod cl-df:copy-from ((format (eql ':postmodern))
                            (input list)
                            &rest options)
  (declare (ignore options))
  (make 'postgres-query :query input))


(defmethod cl-df:to-table ((object postgres-query)
                           &key
                             (key #'identity)
                             (class 'cl-df.table:standard-table)
                             (header-class 'cl-df.header:standard-header)
                             (columns '())
                             (body nil)
                             (header (apply #'cl-df.header:make-header
                                            header-class columns)))
  (cl-df:with-header (header)
    (let* ((column-count (cl-df.header:column-count header))
           (columns (make-array column-count)))
      (iterate
        (for i from 0 below column-count)
        (setf (aref columns i)
              (cl-df.column:make-sparse-material-column
               :element-type (cl-df.header:column-type header i))))
      (let* ((iterator (cl-df.column:make-iterator columns))
             (cl-df.header:*row* (make 'cl-df.table:setfable-table-row
                                       :iterator iterator)))
        (cl-ds:traverse object
                        (lambda (content)
                          (iterate
                            (for i from 0 below column-count)
                            (setf (cl-df.column:iterator-at iterator i)
                                  (funcall key (aref content i)))
                            (unless (null body)
                              (funcall body cl-df.header:*row*))
                            (finally (cl-df.column:move-iterator iterator 1)))))
        (cl-df.column:finish-iterator iterator)
        (make class
              :header header
              :columns columns)))))
