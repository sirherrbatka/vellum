(cl:in-package #:vellum.int)


(defclass postgres-query (cl-ds:traversable)
  ((%query :initarg :query
           :type list
           :reader read-query)))


(defmethod cl-ds:traverse ((object postgres-query) function)
  (bind ((header (vellum.header:header))
         (column-count (vellum.header:column-count header))
         (row (make-array column-count :initial-element :null))
         ((:slots %query) object)
         (query (s-sql:sql-compile %query)))
    (vellum.header:set-row row)
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


(defmethod vellum:copy-from ((format (eql ':postmodern))
                            (input list)
                            &rest options)
  (declare (ignore options))
  (make 'postgres-query :query input))


(defmethod vellum:to-table ((object postgres-query)
                           &key
                             (key #'identity)
                             (class 'vellum.table:standard-table)
                             (header-class 'vellum.header:standard-header)
                             (columns '())
                             (body nil)
                             (header (apply #'vellum.header:make-header
                                            header-class columns)))
  (vellum:with-header (header)
    (let* ((column-count (vellum.header:column-count header))
           (function (if (null body)
                         (constantly nil)
                         (vellum:bind-row-closure body)))
           (columns (make-array column-count)))
      (iterate
        (for i from 0 below column-count)
        (setf (aref columns i)
              (vellum.column:make-sparse-material-column
               :element-type (vellum.header:column-type header i))))
      (let* ((iterator (vellum.column:make-iterator columns))
             (vellum.header:*row* (serapeum:box (make 'vellum.table:setfable-table-row
                                                      :iterator iterator))))
        (cl-ds:traverse object
                        (lambda (content)
                          (iterate
                            (for i from 0 below column-count)
                            (setf (vellum.column:iterator-at iterator i)
                                  (funcall key (aref content i)))
                            (funcall function)
                            (finally (vellum.column:move-iterator iterator 1)))))
        (vellum.column:finish-iterator iterator)
        (make class
              :header header
              :columns columns)))))
