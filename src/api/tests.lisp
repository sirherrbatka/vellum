(cl:in-package #:vellum)

(prove:plan 5)

(let* ((frame-1 (transform (make-table :columns '(a b))
                           (vellum:bind-row (a b)
                             (setf a vellum.table:*current-row*)
                             (setf b (format nil "a~a" a)))
                           :end 5))
       (frame-2 (transform (make-table :columns '(a b))
                           (vellum:bind-row (a b)
                             (setf a vellum.table:*current-row*)
                             (setf b (format nil "b~a" a)))
                           :end 5))
       (result (join :hash :inner
                     `((:frame-1 ,frame-1 a)
                       (:frame-2 ,frame-2 a)))))
  (prove:is (column-count result) 4)
  (prove:is (row-count result) 5))

(let* ((frame-1 (transform (make-table :columns '(a b))
                  (vellum:bind-row (a b)
                    (setf a vellum.table:*current-row*)
                    (setf b (mod a 2)))
                  :end 5))
       (frame-2 (transform (make-table :columns '(a b))
                  (vellum:bind-row (a b)
                    (setf a vellum.table:*current-row*)
                    (setf b (mod a 2)))
                  :end 5))
       (result (join :hash :inner
                     `((:frame-1 ,frame-1 b)
                       (:frame-2 ,frame-2 b)))))
  (prove:is (column-count result) 4)
  (prove:is (row-count result) 13))


(let* ((frame-1 (vellum:to-table '((1 0) (1 1)) :columns '(a b)))
       (frame-2 (vellum:to-table '((1 0) (1 1)) :columns '(a b)))
       (result (vellum:join :hash :inner
                            `((:frame-1 ,frame-1 a)
                              (:frame-2 ,frame-2 b)))))
  (prove:is (row-count result) 2))

(prove:finalize)
