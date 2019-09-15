(in-package #:cl-df)


(defgeneric copy-from (format input &rest options &key &allow-other-keys))

(defgeneric copy-to (format output input &rest options &key &allow-other-keys))
