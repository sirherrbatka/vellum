(cl:in-package #:vellum)


(defgeneric copy-from (format input &rest options &key &allow-other-keys)
  (:method ((format (eql nil)) input &rest options &key body key class header-class columns header)
    (declare (ignore format body key class header-class columns header))
    (apply #'to-table input options)))

(defgeneric copy-to (format output input &rest options &key &allow-other-keys))


(defgeneric join (algorithm method frame-specs &key header class header-class columns
                  &allow-other-keys))


(defgeneric visualize (format data destination &rest options &key))


(defmethod join :before (algorithm method (frame-specs list) &key &allow-other-keys)
  (when (emptyp frame-specs)
    (error 'cl-ds:invalid-argument-value
           :value frame-specs
           :argument frame-specs
           :format-control "Empty frame-specs list, nothing to join.")))
