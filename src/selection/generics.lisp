(cl:in-package #:vellum.selection)


(defgeneric new-stack-frame (previous-stack-frame current-block))
(defgeneric forward* (current-block stack-frame))
(defgeneric overlaps (current-block stack-frame))
(defgeneric make-selection-block* (symbol form))
