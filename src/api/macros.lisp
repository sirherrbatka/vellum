(cl:in-package #:vellum)


(defmacro with-standard-header (columns &body body)
  `(with-header ((vellum:make-header 'vellum:standard-header
                                    ,@(mapcar (curry #'list 'quote)
                                              columns)))
     ,@body))


(defmacro pipeline ((table) &body body)
  (once-only (table)
    `(with-table (,table)
       (~> (cl-ds:whole-range ,table)
           ,@body))))


(defmacro aggregate-rows (table column params &rest more)
  (flet ((aggregator-constructor (expression)
           (bind (((function . body) expression))
             `(cl-ds.alg.meta:aggregator-constructor
               '() nil (function ,function)
               (list '() ,@body)))))
    `(%aggregate-rows ,table
                      ,column
                      (list ,(aggregator-constructor (first params))
                            ,@(rest params))
                      ,@(iterate
                          (for (column (expression . rest)) in (batches more 2))
                          (appending `(,column
                                       (list ,(aggregator-constructor expression)
                                             ,@rest)))))))


(defmacro aggregate-columns (table expression
                             &key
                               name
                               skip-nulls
                               (type t)
                               (predicate ''vellum.header:constantly-t))
  (bind (((function . body) expression))
    `(%aggregate-columns ,table
                         (cl-ds.alg.meta:aggregator-constructor
                          '() nil (function ,function)
                          (list '() ,@body))
                         :skip-nulls ,skip-nulls
                         :type ,type
                         :name ,name
                         :predicate ,predicate)))
