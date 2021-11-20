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
        (assert
         (not (eq (cl-ds.common.abstract:read-ownership-tag column)
                  (cl-ds.common.abstract:read-ownership-tag new-column))))))
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


(declaim (inline transform-row-impl))
(defun transform-row-impl (transformation
                           &optional
                             (function (standard-transformation-bind-row-closure
                                        transformation))
                             (move-iterator t)
                             transform-control)
  (declare (type standard-transformation transformation)
           (optimize (speed 3)))
  (cl-ds.utils:with-slots-for (transformation standard-transformation)
    (bind ((prev-control (ensure-function *transform-control*))
           ((:flet move-iterator ())
            (incf count)
            (when move-iterator
              (vellum.column:move-iterator iterator 1)))
           (index (vellum.column:sparse-material-column-iterator-index iterator))
           (*transform-control*
            (or transform-control
                (lambda (operation)
                  (cond ((eq operation :drop)
                         #1=(iterate
                              (declare (type fixnum i))
                              (for i from 0 below column-count)
                              (setf (vellum.column:iterator-at iterator i)
                                    :null)
                              (finally
                               (setf (vellum.column:column-at marker-column index) t
                                     dropped t)
                               (move-iterator)
                               (return-from transform-row-impl transformation))))
                        ((eq operation :finish)
                         (funcall prev-control operation)
                         (return-from transform-row-impl transformation))
                        ((eq operation :nullify)
                         (iterate
                           (declare (type fixnum i))
                           (for i from 0 below column-count)
                           (setf (vellum.column:iterator-at iterator i) :null)))
                        (t (funcall prev-control operation)))))))
      (tagbody main
         (unless restarts-enabled
           (funcall function row)
           (go end))
         (restart-case (handler-case (funcall function row)
                         (error (e)
                           (error 'transformation-error
                                  :cause e)))
           (skip-row ()
             :report "Omit this row."
             (vellum.column:untouch iterator)
             (go end))
           (retry ()
             :report "Retry calling function on this row."
             (vellum.column:untouch iterator)
             (go main))
           (drop-row ()
             :report "Drop this row."
             #1#))
       end)
      (move-iterator)
      transformation)))


(defun select-columns (frame selection)
  (vellum.header:with-header ((header frame))
    (bind ((header (header frame))
           (columns (read-columns frame))
           (column-indexes
             (~> selection
                 (vellum.selection:address-range
                  (lambda (spec)
                    (vellum.header:ensure-index header
                                                (if (listp spec)
                                                    (first spec)
                                                    spec)))
                  (column-count frame))
                 cl-ds.alg:to-vector))
           ((:values new-header old-ids)
            (vellum.header:select-columns header column-indexes))
           (new-columns (map 'vector
                             (compose (rcurry #'cl-ds:replica t)
                                      (curry #'aref columns)
                                      (curry #'vellum.header:ensure-index
                                             header))
                             old-ids)))
      (assert (= (length column-indexes) (length new-columns)))
      (cl-ds.utils:quasi-clone* frame
        :header new-header
        :columns new-columns))))


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
            (vellum.selection:address-range
             selection
             (lambda (x)
               (when (typep x '(or string symbol))
                 (error 'vellum.selection:name-when-selecting-row
                        :value x
                        :format-control "Attempting to access row by a non-integer value: ~a"
                        :format-arguments `(,x)))
               x)
             row-count))
      (with iterator = (make-iterator new-columns))
      (for source-iterator = (iterator frame t))
      (for (values value more) = (cl-ds:consume-front selection))
      (while more)
      (vellum.column:move-iterator-to source-iterator value)
      (iterate
        (declare (type fixnum column-index))
        (for column-index from 0 below column-count)
        (setf (vellum.column:iterator-at iterator column-index)
              (vellum.column:iterator-at source-iterator column-index)))
      (vellum.column:move-iterator iterator 1)
      (finally (vellum.column:finish-iterator iterator)))
    (cl-ds.utils:quasi-clone* frame
      :columns new-columns)))


(defun table-from-header (class header)
  (make class
        :header header
        :columns (iterate
                   (with column-count = (vellum.header:column-count header))
                   (with result = (make-array column-count))
                   (for i from 0 below column-count)
                   (setf (aref result i)
                         (vellum.column:make-sparse-material-column
                          :element-type (vellum.header:column-type header i)))
                   (finally (return result)))))


(defun transform-impl (frame bind-row restarts-enabled in-place start end)
  (with-table (frame)
    (let* ((done nil)
           (transformation (transformation frame
                                           bind-row
                                           :start start
                                           :restarts-enabled restarts-enabled
                                           :in-place in-place))
           (row (standard-transformation-row transformation))
           (*transform-control*
             (lambda (operation)
               (cond ((eq operation :finish)
                      (setf done t))
                     (t (funcall *transform-control* operation))))))
      (vellum.header:set-row row)
      (iterate
        (declare (type fixnum *current-row*))
        (for *current-row* from start)
        (until (or done
                   (and (not (null end))
                        (>= *current-row* end))))
        (transform-row-impl transformation))
      (transformation-result transformation))))


(defun parallel-transform-impl (frame bind-row in-place start end)
  (bind ((done nil)
         (iterator (iterator frame in-place))
         (transformations (iterate
                            (with result = (make-array cl-ds.common.rrb:+maximum-children-count+))
                            (for i from 0 below cl-ds.common.rrb:+maximum-children-count+)
                            (setf (aref result i)
                                  (transformation frame
                                                  bind-row
                                                  :start start
                                                  :offset i
                                                  :iterator iterator
                                                  :restarts-enabled nil
                                                  :in-place in-place))))
         (main-lock (bt:make-lock))
         (done-index most-positive-fixnum)
         (transform-control
          (lambda (operation)
            (cond ((eq operation :finish)
                   (bt:with-lock-held (main-lock)
                     (setf done t)
                     (minf done-index *current-row*)))
                  (t (funcall *transform-control* operation)))))
         (current-row start)
         (columns (vellum.column:columns iterator))
         ((:flet transform-row-impl (transformation))
          (with-table (frame)
            (let* ((*transform-control* transform-control)
                   (row (standard-transformation-row transformation))
                   (offset (table-row-offset row))
                   (current-index (+ current-row offset))
                   (*current-row* current-index)
                   (function (standard-transformation-bind-row-closure transformation)))
              (unless (or (and (not (null end))
                               (>= current-index end))
                          (bt:with-lock-held (main-lock)
                            (and done
                                 (>= current-index done-index))))
                (vellum.header:set-row row)
                (transform-row-impl transformation function nil))))))
    (iterate
      (declare (type fixnum *current-row*))
      (for i from start by cl-ds.common.rrb:+maximum-children-count+)
      (setf current-row i)
      (until (or done
                 (and (not (null end))
                      (>= current-row end))))
      (lparallel:pmap nil #'transform-row-impl transformations))
    (vellum.column:finish-iterator iterator)
    (let ((new-columns (vellum.column:columns iterator)))
      (when (some #'standard-transformation-dropped transformations)
        cl-ds.utils:todo)
      (if in-place
          (progn
            (write-columns new-columns frame)
            frame)
          (cl-ds.utils:quasi-clone* frame
            :columns (ensure-replicas columns new-columns))))))
