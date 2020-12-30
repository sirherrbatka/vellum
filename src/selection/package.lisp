(cl:in-package #:cl-user)


(defpackage #:vellum.selection
  (:use #:cl #:vellum.aux-package)
  (:export
   #:fold-selection-input
   #:name-when-selecting-row
   #:selection-syntax-error
   #:next-position))
