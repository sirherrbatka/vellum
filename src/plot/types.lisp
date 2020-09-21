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


(defclass aesthetics-layer (fundamental-layer)
  ((%x :initarg :x
       :reader x)
   (%y :initarg :y
       :reader y)
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
