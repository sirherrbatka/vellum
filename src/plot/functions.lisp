(cl:in-package #:vellum.plot)


(defun aesthetics (&key x y color shape size label label-position)
  (make 'aesthetics-layer
        :x x
        :y y
        :label label
        :label-position label-position
        :color color
        :shape shape
        :size size))


(defun mapping (&key x y z color shape size label label-position)
  (make 'mapping-layer
        :x x
        :y y
        :z z
        :label label
        :label-position label-position
        :color color
        :shape shape
        :size size))


(defun scale ()
  cl-ds.utils:todo)


(defun points ()
  (make 'points-geometrics))


(defun line ()
  (make 'line-geometrics))


(defun heatmap ()
  (make 'heatmap-geometrics))


(defun boxes ()
  cl-ds.utils:todo)


(defun statistics ()
  cl-ds.utils:todo)


(defun coordinates ()
  cl-ds.utils:todo)


(defun stack (data-layer &rest layers)
  (reduce #'add layers :initial-value data-layer))
