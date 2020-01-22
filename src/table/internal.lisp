(in-package #:cl-df.table)


(defun insert-tail (column)
  (~>> column cl-ds.common.abstract:read-ownership-tag
       (cl-ds.dicts.srrb:transactional-insert-tail! column)))


(defun column-transformation-closure (in-place)
  (if in-place
      (lambda (column)
        (lret ((result (cl-ds:replica column nil)))
          (insert-tail result)))
      (lambda (column)
        (lret ((result (cl-ds:replica column t)))
          (insert-tail result)))))


(defun make-iterator (columns &key (transformation #'identity))
  (cl-df.column:make-iterator columns :transformation transformation))


(defun ensure-replicas (columns new-columns &optional (isolate t))
  (iterate
    (for i from 0 below (length new-columns))
    (for new-column = (aref new-columns i))
    (for column = (aref columns i))
    (if (eq column new-column)
        (setf (aref new-columns i)
              (cl-ds:replica new-column isolate))
        (progn
          (assert
           (not (eq (cl-ds.common.abstract:read-ownership-tag column)
                    (cl-ds.common.abstract:read-ownership-tag new-column))))
          (assert
           (not (eq (cl-ds.dicts.srrb:access-tree column)
                    (cl-ds.dicts.srrb:access-tree new-column)))))))
  new-columns)


(defun remove-nulls-from-columns (columns &optional (transform #'identity))
  (bind ((column-count (length columns)))
    (when (zerop column-count)
      (return-from remove-nulls-from-columns columns))
    (let* ((iterator (make-iterator columns :transformation transform))
           (new-columns (cl-df.column:columns iterator)))
      (assert (not (eq new-columns columns)))
      (cl-df.column:remove-nulls iterator)
      new-columns)))


(defun transform-row-impl (transformation function)
  (declare (type standard-transformation transformation)
           (type function function)
           (optimize (speed 3) (safety 0)))
  (cl-ds.utils:with-slots-for (transformation standard-transformation)
    (let* ((prev-control (ensure-function *transform-control*))
           (*transform-control*
             (lambda (operation)
               (cond ((eq operation :drop)
                     (iterate
                       (declare (type fixnum i))
                       (for i from 0 below column-count)
                       (setf (cl-df.column:iterator-at iterator i) :null))
                     (setf (cl-df.column:iterator-at marker-iterator 0) t
                           dropped t))
                    ((eq operation :nullify)
                     (iterate
                       (declare (type fixnum i))
                       (for i from 0 below column-count)
                       (setf (cl-df.column:iterator-at iterator i) :null)))
                    (t (funcall prev-control operation))))))
      (funcall function)
      (incf count)
      (cl-df.column:move-iterator iterator 1)
      (cl-df.column:move-iterator marker-iterator 1)
      transformation)))
