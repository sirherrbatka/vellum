(cl:in-package #:vellum.plot)


(defclass stack-of-layers ()
  ((%data :reader data-layer
          :initarg :data-layer)
   (%mapping :reader mapping-layer
             :initarg :mapping-layer)
   (%aesthetics :reader aesthetics-layer
                :initarg :aesthetics-layer)
   (%scale :reader scale-layer
           :initarg :scale-layer)
   (%geometrics :reader geometrics-layer
                :initarg :geometrics-layer)
   (%statistics :reader statistics-layer
                :initarg :statistics-layer)
   (%facets-layer :reader facets-layer
                  :initarg :facets-layer)
   (%coordinates :reader coordinates-layer
                 :initarg :coordinates-layer))
  (:default-initargs
   :data-layer nil
   :mapping-layer nil
   :facets-layer nil
   :aesthetics-layer nil
   :scale-layer nil
   :geometrics-layer nil
   :statistics-layer nil
   :coordinates-layer nil))


(defclass fundamental-layer ()
  ())


(defclass geometrics-layer (fundamental-layer)
  ((%mapping :initarg :mapping
             :reader mapping)))


(defclass heatmap-geometrics (geometrics-layer)
  ())


(defclass points-geometrics (geometrics-layer)
  ())


(defclass line-geometrics (geometrics-layer)
  ())


(defclass boxes-geometrics (geometrics-layer)
  ())


(defclass mapping-layer (fundamental-layer)
  ((%x :initarg :x
       :reader x)
   (%y :initarg :y
       :reader y)
   (%z :initarg :z
       :reader z)
   (%color :initarg :color
           :reader color)
   (%shape :initarg :shape
           :reader shape)
   (%label :initarg :label
           :reader label)
   (%label-position :initarg :label-position
                    :reader label-position)
   (%size :initarg :size
          :reader size)))


(defclass axis ()
  ((%label :initarg :label
           :reader label)
   (%scale-anchor :initarg :scale-anchor
                  :reader scale-anchor)
   (%scale-ratio :initarg :scale-ratio
                 :reader scale-ratio)
   (%constrain :initarg :constrain
               :reader constrain)
   (%tick-length :initarg :tick-length
                 :reader tick-length)
   (%range :initarg :range
           :reader range)
   (%dtick :initarg :dtick
           :reader dtick)))


(defclass aesthetics-layer (fundamental-layer)
  ((%x :initarg :x
       :reader x)
   (%y :initarg :y
       :reader y)
   (%height :initarg :height
            :reader height)
   (%width :initarg :width
           :reader width)
   (%color :initarg :color
           :reader color)
   (%shape :initarg :shape
           :reader shape)
   (%label :initarg :label
           :reader label)
   (%label-position :initarg :label-position
                    :reader label-position)
   (%size :initarg :size
          :reader size)))
