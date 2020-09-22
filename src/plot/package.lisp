(cl:in-package #:cl-user)


(defpackage #:vellum.plot
  (:use #:cl #:vellum.aux-package)
  (:export
   #:visualize
   #:aesthetics
   #:points
   #:stack
   #:line
   #:mapping
   #:heatmap
   #:axis
   #:add))
