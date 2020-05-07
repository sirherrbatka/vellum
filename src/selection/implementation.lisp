(cl:in-package #:vellum.selection)


(defun forward (stack-frame)
  (forward* (read-current-block stack-frame)
            stack-frame))


(defun ensure-index (alias-or-index)
  (handler-case
      (if (integerp alias-or-index)
          alias-or-index
          (vellum.header:alias-to-index (vellum.header:header)
                                        alias-or-index))
    (vellum.header:no-header (e)
      (declare (ignore e))
      (error 'alias-when-selecting-row
             :value alias-or-index
             :format-control "Attempting to access row by a non-integer value: ~a"
             :format-arguments `(,alias-or-index)))))


(defmethod shared-initialize :after ((object bounded-selection-block)
                                     slots
                                     &rest arguments)
  (declare (ignore slots arguments))
  (let ((from (read-from object))
        (to (read-to object)))
    (setf (slot-value object '%to)
          (if (null to)
              most-positive-fixnum
              (ensure-index to)))
    (setf (slot-value object '%from) (ensure-index from))))


(defmethod shared-initialize :after ((object value-selection-block)
                                     slots
                                     &rest arguments)
  (declare (ignore slots arguments))
  (setf (slot-value object '%value)
        (ensure-index (read-value object))))


(defmethod print-object ((object bracket-selection-block) stream)
  (print-unreadable-object (object stream :type t)
    (format stream "~{~a~^ ~}" (read-children object))))


(defmethod print-object ((block value-selection-block) stream)
  (print-unreadable-object (block stream :type t)
    (format stream "~a" (read-value block))))


(defun index (stack-frame)
  (car (access-index stack-frame)))


(defun (setf index) (new-value stack-frame)
  (setf (car (access-index stack-frame)) new-value))


(defmethod new-stack-frame (previous-stack-frame (current-block fundamental-selection-block))
  (make 'stack-frame
        :index (access-index previous-stack-frame)
        :previous-frame previous-stack-frame
        :current-block current-block
        :state nil))


(defmethod new-stack-frame (previous-stack-frame (current-block bracket-selection-block))
  (make 'stack-frame
        :index (access-index previous-stack-frame)
        :previous-frame previous-stack-frame
        :current-block current-block
        :state (read-children current-block)))


(defmethod new-stack-frame (previous-stack-frame (current-block root-selection-block))
  (make 'stack-frame
        :index (list -1)
        :previous-frame previous-stack-frame
        :current-block current-block
        :state (read-children current-block)))


(defun next-position (selection)
  (iterate
    (setf #1=(access-stack selection) (forward #1#))
    (for stack-frame = #1#)
    (until (null stack-frame))
    (for value = (access-value stack-frame))
    (cond ((null stack-frame)
           (leave nil))
          ((null value)
           (next-iteration))
          (t (leave value)))))


(defun first-atom (form)
  (if (atom form)
      form
      (first-atom (first form))))


(defun make-selection-block (form)
  (let ((symbol (first-atom form)))
    (make-selection-block* symbol form)))


(defmethod make-selection-block* (symbol form)
  (bind (((arguments children) form)
         (result
          (apply #'make (matching-block-class symbol)
                 :parent nil
                 :children children
                 arguments)))
    (iterate
      (for child in children)
      (setf (access-parent child) result))
    result))


(defmethod make-selection-block* ((symbol (eql :v)) form)
  (make 'value-selection-block
        :value (second form)))


(defmethod make-selection-block* ((symbol (eql :sampling)) form)
  (make 'sampling-selection-block
        :sampling-rate (second form)))


(defun fold-selection-input (input)
  (bind ((batches (batches input 2))
         ((:labels fold (opening list))
          (iterate
            (with result = '())
            (until (endp list))
            (for (label value) = (first list))
            (cond ((opening-p label)
                   (bind (((:values tree rest)
                           (fold (first list) (rest list))))
                     (push tree result)
                     (setf list rest)
                     (next-iteration)))
                  ((matching-closing-p (first opening) label)
                   (leave (values (make-selection-block
                                   (list (append opening
                                                 (first list))
                                         (reverse result)))
                                  (rest list))))
                  ((closing-p label)
                   (leave (values (make-selection-block
                                   (list (first list)
                                         (reverse result)))
                                  (rest list))))
                  (t (push (make-selection-block (first list))
                           result)
                     (pop list)))
            (finally (return (values (reverse result)
                                     '())))))
         (folding-result (fold nil batches))
         (top-level (if (listp folding-result)
                        folding-result
                        (list folding-result)))
         (root (make 'root-selection-block :children top-level
                                           :parent nil)))
    (iterate
      (for elt in top-level)
      (setf (access-parent elt) root))
    (make 'selection :stack (new-stack-frame nil root))))


(defmethod overlaps ((current-block sampling-selection-block)
                     stack-frame)
  t)


(defmethod overlaps ((current-block bounded-selection-block)
                     stack-frame)
  (>= (1+ (index stack-frame))
      (read-from current-block)))


(defmethod overlaps ((current-block value-selection-block)
                     stack-frame)
  t)


(defmethod forward* ((current-block take-selection-block)
                     stack-frame)
  (let ((index (index stack-frame))
        (from (read-from current-block))
        (to (read-to current-block))
        (state (access-state stack-frame)))
    (cond ((>= index to)
           (forward (read-previous-frame stack-frame)))
          ((< index from)
           (setf (index stack-frame) from
                 (access-value stack-frame) from)
           stack-frame)
          ((and (not (endp state))
                (overlaps (first state)
                          stack-frame))
           (forward (new-stack-frame stack-frame
                                     (pop (access-state stack-frame)))))
          (t (let ((new-index (1+ (index stack-frame))))
               (setf (index stack-frame) new-index
                     (access-value stack-frame) new-index)
               stack-frame)))))


(defmethod forward* ((current-block sampling-selection-block)
                     stack-frame)
  (let* ((index (index stack-frame))
         (sampling-rate (read-sampling-rate current-block))
         (parent (access-parent current-block))
         (to (cond ((typep parent 'root-selection-block)
                    most-positive-fixnum)
                   ((typep parent 'take-selection-bock)
                    (read-to parent))
                   (t (error 'selection-syntax-error
                             :format-control "Selection must be nested in the take block."
                             :format-arguments '())))))
    (iterate
      (for i from (1+ index) below to)
      (setf (index stack-frame) i)
      (for take = (<= (random 1.0) sampling-rate))
      (when take
        (setf (access-value stack-frame) i)
        (leave stack-frame))
      (finally (return (forward (read-previous-frame stack-frame)))))))


(defmethod forward* ((current-block skip-selection-block)
                     stack-frame)
  (let* ((index (index stack-frame))
         (from (read-from current-block))
         (to (read-to current-block))
         (state (access-state stack-frame))
         (previous-stack-frame (read-previous-frame stack-frame))
         (parent (access-parent current-block))
         (previous-state (access-state previous-stack-frame)))
    (cond ((or (< (1+ index) from)
                (and (endp previous-state)
                     (typep parent 'root-selection-block)
                     (>= index to)))
           (setf (access-value stack-frame)
                 (incf (index stack-frame)))
            stack-frame)
           ((>= index to)
            (forward (read-previous-frame stack-frame)))
          ((and (not (endp state))
                (overlaps (first state)
                          stack-frame))
           (forward (new-stack-frame stack-frame
                                     (pop (access-state stack-frame)))))
          (t (setf (access-value stack-frame) nil)
             (incf (index stack-frame))
             stack-frame))))


(defmethod forward* ((current-block value-selection-block)
                     stack-frame)
  (if (access-state stack-frame)
      (forward (read-previous-frame stack-frame))
      (progn
        (setf (access-value stack-frame) (read-value current-block)
              (access-state stack-frame) t)
        stack-frame)))


(defmethod forward* ((current-block root-selection-block)
                     stack-frame)
  (let ((state (access-state stack-frame)))
    (if (endp state)
        nil
        (forward (new-stack-frame stack-frame
                                  (pop (access-state stack-frame)))))))
