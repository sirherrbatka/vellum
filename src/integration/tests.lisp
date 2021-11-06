(cl:in-package #:vellum.int)

(prove:plan 1)

(let* ((source-data '((1) (2) (3) (4)))
       (table1 (vellum:to-table source-data
                                :columns '(c)))
       (table2 (vellum:pipeline (table1)
                 (cl-ds.alg:group-by
                  :key (compose #'evenp
                                (vellum:brr c)))
                 (cl-ds.alg:to-list :key (vellum:brr c))
                 (vellum:to-table
                  :body (vellum:bind-row (d)
                          (setf d
                                (cl-ds.utils:transform #'1+ d)))
                  :columns '(even d))))
       (odd-list (vellum:transform table2
                    (vellum:bind-row (even)
                      (vellum:drop-row-when even)))))
  (prove:is (vellum:at odd-list 0 'd) '(2 4) :test 'equal))


(prove:finalize)
