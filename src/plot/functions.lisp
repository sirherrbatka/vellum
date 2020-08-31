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


(defun mapping (&key x y color shape size label label-position)
  (make 'mapping-layer
        :x x
        :y y
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


(defun mapping ())


(defun boxes ()
  cl-ds.utils:todo)


(defun statistics ()
  cl-ds.utils:todo)


(defun coordinates ()
  cl-ds.utils:todo)

