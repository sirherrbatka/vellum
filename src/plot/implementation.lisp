(cl:in-package #:vellum.plot)


(defmethod cl-ds.utils:cloning-information append
    ((stack stack-of-layers))
  `((:data-layer data-layer)
    (:aesthetics-layer aesthetics-layer)
    (:scale-layer scale-layer)
    (:geometrics-layers geometrics-layers)
    (:statistics-layer statistics-layer)
    (:facets-layer facets-layer)
    (:coordinates-layer coordinates-layer)))


(defmethod add ((data vellum.table:fundamental-table)
                (layer fundamental-layer))
  (make 'stack-of-layers
        :data-layer data
        (layer-category layer)
        layer))


(defmethod layer-category ((layer geometrics-layer))
  :geometrics-layer)


(defmethod layer-category ((layer aesthetics-layer))
  :aesthetics-layer)


(defmethod layer-category ((layer mapping-layer))
  :mapping-layer)


(defmethod add ((stack stack-of-layers)
                (layer fundamental-layer))
  (cl-ds.utils:quasi-clone stack
                           (layer-category layer)
                           layer))


(defmethod add ((stack stack-of-layers)
                (layer geometrics-layer))
  (cl-ds.utils:quasi-clone
   stack
   :geometrics-layers (cons layer (geometrics-layers stack))))


(defmethod visualize ((backend (eql :plotly))
                      (stack stack-of-layers)
                      (destination stream))
  (plotly-visualize stack destination))


(defmethod visualize ((backend (eql :plotly))
                      (stack stack-of-layers)
                      destination)
  (with-output-to-file (stream destination)
    (visualize :plotly stack stream)))
