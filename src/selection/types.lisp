(cl:in-package #:vellum.selection)


(defclass stack-frame ()
  ((%state :initarg :state
           :accessor access-state)
   (%current-block :initarg :current-block
                   :reader read-current-block)
   (%index :initarg :index
           :accessor access-index)
   (%value :initarg :value
           :accessor access-value)
   (%previous-frame :initarg :previous-frame
                    :reader read-previous-frame))
  (:default-initargs :index (list -1)
                     :state nil
                     :value nil
                     :previous-frame nil))


(defclass selection ()
  ((%stack :initarg :stack
           :accessor access-stack)))


(defclass fundamental-selection-block ()
  ((%parent :initarg :parent
            :accessor access-parent)))


(defclass bracket-selection-block (fundamental-selection-block)
  ((%children :initarg :children
              :type list
              :reader read-children)))


(defclass root-selection-block (bracket-selection-block)
  ())


(defclass bounded-selection-block (bracket-selection-block)
  ((%from :initarg :from
          :reader read-from)
   (%to :initarg :to
        :reader read-to))
  (:default-initargs :from 0
                     :to nil))


(defclass skip-selection-block (bounded-selection-block)
  ((%from :initarg :skip-from)
   (%to :initarg :skip-to)))


(defclass take-selection-block (bounded-selection-block)
  ((%from :initarg :take-from)
   (%to :initarg :take-to)))


(defclass value-selection-block (fundamental-selection-block)
  ((%value :initarg :value
           :reader read-value)))
