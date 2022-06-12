(cl:in-package #:vellum.table)


(defgeneric hstack* (frame more-frames &key isolate))

(defgeneric vstack* (frame more-frames))

(defgeneric select (frame &key columns rows))

(defgeneric vmask (frame mask &key in-place))

(defgeneric at (frame row column))

(defgeneric (setf at) (new-value frame row column))

(defgeneric transform (frame function
                       &key in-place start end restarts-enabled aggregated-output))

(defgeneric iterator (frame in-place))

(defgeneric transformation (frame bind-row &key in-place start restarts-enabled aggregated-output))

(defgeneric transform-row (transformation &optional bind-row-closure))

(defgeneric transformation-result (transformation))

(defgeneric row-erase (row))

(defgeneric column-count (frame))

(defgeneric row-count (frame))

(defgeneric column-name (frame column))

(defgeneric column-type (frame column))

(defgeneric column-at (frame column))

(defgeneric header (frame))

(defgeneric remove-nulls (frame &key in-place))

(defgeneric make-table* (class &optional header))

(defgeneric erase! (frame row column))

(defgeneric show (as table &key &allow-other-keys))

(defgeneric alter-columns (table &rest columns))

(defgeneric bind-row-closure (bind-row-object &key header aggregated-output))

(defgeneric read-iterator (object))
