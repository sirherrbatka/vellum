(cl:in-package #:vellum.plot)


(defun plotly-extract-data (table column)
  (if (null column)
      nil
      (vellum.table:with-table (table)
        (cl-ds.alg:to-list
         table
         :key (lambda (&rest ignored)
                (declare (ignore ignored))
                (let ((content (vellum.header:rr column)))
                  (typecase content
                    (double-float (coerce content 'single-float))
                    (string (format nil "'~a'" content))
                    (t content))))))))


(defgeneric plotly-mode (geometrics mapping)
  (:method ((geometrics points-geometrics) mapping)
    (if (label mapping) "markers+text" "markers"))
  (:method ((geometrics line-geometrics) mapping)
    (if (label mapping) "markers+lines" "lines"))
  (:method ((geometrics heatmap-geometrics) mapping)
    nil))


(defgeneric plotly-type (geometrics)
  (:method ((geometrics points-geometrics))
    "scatter")
  (:method ((geometrics heatmap-geometrics))
    "heatmap")
  (:method ((geometrics line-geometrics))
    "scatter"))


(defun plotly-format (stream field-name field-value)
  (format stream "'~a': ~a, ~%" field-name
          (etypecase field-value
            (symbol (format nil "'~(~a~)'" (symbol-name field-value)))
            (list (format nil "[~{~a~^, ~}]" field-value))
            (float (format nil "~F" field-value))
            (string (format nil "'~a'" field-value))
            (integer field-value))))


(defun plotly-format-no-nulls (stream field-name field-value)
  (if (null field-value)
      nil
      (progn
        (plotly-format stream field-name field-value)
        t)))


(defun plotly-format-pick (stream field-a value-a field-b value-b)
  (or (plotly-format-no-nulls stream field-a value-a)
      (plotly-format-no-nulls stream field-b value-b)))


(defun plotly-generate-data (stack)
  (bind ((geometrics (geometrics-layer stack))
         (mapping (mapping-layer stack))
         (data (data-layer stack))
         (aesthetics (aesthetics-layer stack))
         (x (x mapping))
         (y (y mapping))
         (z (z mapping))
         (color (color mapping))
         (shape (shape mapping))
         (label (label mapping))
         (label-position (label-position aesthetics))
         (size (size mapping)))
    (with-output-to-string (stream)
      (format stream "{")
      (plotly-format stream "x" (plotly-extract-data data x))
      (plotly-format stream "y" (plotly-extract-data data y))
      (unless (null z)
        (plotly-format stream "z" (plotly-extract-data data z)))
      (plotly-format-no-nulls stream "mode"
                              (plotly-mode geometrics mapping))
      (plotly-format stream "type" (plotly-type geometrics))
      (format stream "marker: {")
      (plotly-format-no-nulls stream "size" (plotly-extract-data data size))
      (plotly-format-no-nulls stream "text" (plotly-extract-data data label))
      (plotly-format-no-nulls stream "textposition" label-position)
      (format stream "},")
      (unless (null aesthetics)
        ;; should also handle color here
        ;; should also handle shape here
        (plotly-format-no-nulls stream "name" (label aesthetics)))
      (format stream "}"))))


(defun plotly-format-axis (stream axis)
  (when axis
    (plotly-format-no-nulls stream "scaleanchor" (scale-anchor axis))
    (plotly-format-no-nulls stream "range" (range axis))
    (plotly-format-no-nulls stream "constrain" (constrain axis))
    (plotly-format-no-nulls stream "scaleratio" (scale-ratio axis))
    (plotly-format-no-nulls stream "ticklen" (tick-length axis))
    (plotly-format-no-nulls stream "dtick" (dtick axis))))


(defun plotly-generate-layout (stack)
  (bind ((aesthetics (aesthetics-layer stack))
         (xaxis (x aesthetics))
         (yaxis (y aesthetics))
         (mapping (mapping-layer stack)))
    (with-output-to-string (stream)
      (format stream "{")
      (plotly-format-no-nulls stream "height"
                              (height aesthetics))
      (plotly-format-no-nulls stream "width"
                              (width aesthetics))
      (plotly-format-no-nulls stream "title"
                              (label aesthetics))
      (format stream "xaxis: {")
      (format stream "title: {")
      (if (and xaxis (label xaxis))
          (plotly-format-no-nulls stream "text" (label xaxis))
          (plotly-format-no-nulls stream "text" (x mapping)))
      (format stream "},~%")
      (plotly-format-axis stream xaxis)
      (format stream "},~%")
      (format stream "yaxis: {")
      (format stream "title: {")
      (if (and yaxis (label yaxis))
          (plotly-format-no-nulls stream "text" (label yaxis))
          (plotly-format-no-nulls stream "text" (y mapping)))
      (format stream "},~%")
      (plotly-format-axis stream yaxis)
      (format stream "}~%")
      (format stream "}~%"))))


(defun plotly-visualize (stack stream)
  (let ((layout (plotly-generate-layout stack))
        (data (plotly-generate-data stack)))
    (format stream "<html>~%")
    (format stream "<head>~%")
    (format stream "<script src='https://cdn.plot.ly/plotly-latest.min.js'></script></head>~%")
    (format stream "<body>")
    (format stream
            "<div id='plotDiv'><!-- Plotly chart will be drawn inside this DIV --></div>~%")
    (format stream "<script type='text/javascript'>~%")
    (format stream "Plotly.newPlot('plotDiv', [~a], ~a);~%"
            data
            layout)
    (format stream "</script>~%")
    (format stream "</body>")
    (format stream "</html>~%")))
