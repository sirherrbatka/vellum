(cl:in-package #:vellum.table)


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
  (vellum.column:make-iterator columns :transformation transformation))


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
           (new-columns (vellum.column:columns iterator)))
      (assert (not (eq new-columns columns)))
      (vellum.column:remove-nulls iterator)
      new-columns)))


(defun transform-row-impl (transformation function)
  (declare (type standard-transformation transformation)
           (type function function))
  (cl-ds.utils:with-slots-for (transformation standard-transformation)
    (bind ((prev-control (ensure-function *transform-control*))
           ((:flet move-iterator ())
            (incf count)
            (vellum.column:move-iterator iterator 1)
            (vellum.column:move-iterator marker-iterator 1))
           (*transform-control*
             (lambda (operation)
               (cond ((eq operation :drop)
                      (iterate
                        (declare (type fixnum i))
                        (for i from 0 below column-count)
                        (setf (vellum.column:iterator-at iterator i) :null))
                      (setf (vellum.column:iterator-at marker-iterator 0) t
                            dropped t)
                      (move-iterator)
                      (return-from transform-row-impl transformation))
                     ((eq operation :finish)
                      (funcall prev-control operation)
                      (return-from transform-row-impl transformation))
                     ((eq operation :nullify)
                      (iterate
                        (declare (type fixnum i))
                        (for i from 0 below column-count)
                        (setf (vellum.column:iterator-at iterator i) :null)))
                     (t (funcall prev-control operation))))))
      (funcall function)
      (move-iterator)
      transformation)))


(defun select-columns (frame selection)
  (let* ((header (header frame))
         (columns (read-columns frame))
         (column-count (length columns))
         (column-indexes
           (vellum.header:with-header ((header frame))
             (iterate
               (with result = (vect))
               (with selection =
                     (vellum.selection:fold-selection-input selection))
               (for value = (vellum.selection:next-position selection))
               (while (and value (< value column-count)))
               (vector-push-extend value result)
               (finally (return result)))))
         (new-header (vellum.header:select-columns header column-indexes))
         (new-columns (map 'vector (compose (rcurry #'cl-ds:replica t)
                                            (curry #'aref columns))
                           column-indexes)))
    (declare (type simple-vector columns new-columns)
             (optimize (debug 3)))
    (cl-ds.utils:quasi-clone* frame
      :header new-header
      :columns new-columns)))


(defun select-rows (frame selection)
  (bind ((columns (read-columns frame))
         (row-count (row-count frame))
         (column-count (length columns))
         (new-columns (map 'vector
                           (lambda (x)
                             (vellum.column:make-sparse-material-column
                              :element-type (vellum.column:column-type x)))
                           columns)))
    (declare (type simple-vector new-columns columns)
             (type fixnum column-count))
    (when (emptyp new-columns)
      (return-from select-rows
        (cl-ds.utils:quasi-clone* frame
          :columns new-columns)))
    (iterate
      (with selection =
            (vellum.selection:fold-selection-input selection))
      (with iterator = (make-iterator new-columns))
      (for source-iterator = (iterator frame t))
      (for value = (vellum.selection:next-position selection))
      (for previous-value previous value initially 0)
      (while (and value (< value row-count)))
      (vellum.column:move-iterator-to source-iterator value)
      (iterate
        (declare (type fixnum column-index))
        (for column-index from 0 below column-count)
        (for column = (aref new-columns column-index))
        (setf (vellum.column:iterator-at iterator column-index)
              (vellum.column:iterator-at source-iterator column-index)))
      (vellum.column:move-iterator iterator 1)
      (finally (vellum.column:finish-iterator iterator)))
    (cl-ds.utils:quasi-clone* frame
      :columns new-columns)))
