(cl:in-package #:vellum.plot)


(defvar *json-stream*)
(defvar *depth* 0)


(defmacro slot (name content)
  (with-gensyms (!output)
    `(let ((*depth* (1+ *depth*))
           (,!output (with-output-to-string (*json-stream*)
                       ,content)))
       (unless (emptyp ,!output)
         (format *json-stream* "'~a': ~a" ,name ,!output)
         (format *json-stream* ",~%")))))


(defmacro value (content)
  `(let ((result (ignore-errors ,content)))
     (unless (null result)
       (json-format *json-stream* result))))


(defun var (content)
  (format *json-stream* "~a" content))


(defmacro object (&body content)
  `(progn
     (format *json-stream* "{")
     ,@content
     (format *json-stream* "}")))


(defmacro json ((stream) &body body)
  `(let ((*json-stream* ,stream))
     ,@body))


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


(defun json-format (stream field-value)
  (format stream "~a"
          (etypecase field-value
            (symbol (format nil "'~(~a~)'" (symbol-name field-value)))
            (list (format nil "[~{~a~^, ~}]" field-value))
            (float (format nil "~F" field-value))
            (string (format nil "'~a'" field-value))
            (integer field-value))))


(defun plotly-format (stream field-name field-value)
  (format stream "'~a': ~a, ~%" field-name
          (etypecase field-value
            (symbol (format nil "'~(~a~)'" (symbol-name field-value)))
            (list (format nil "[~{~a~^, ~}]" field-value))
            (float (format nil "~F" field-value))
            (string (format nil "'~a'" field-value))
            (integer field-value))))


(defun plotly-format-no-nulls (field-name field-value)
  (if (null field-value)
      nil
      (progn
        (plotly-format *json-stream* field-name field-value)
        t)))


(defun plotly-format-pick (stream field-a value-a field-b value-b)
  (or (plotly-format-no-nulls stream field-a value-a)
      (plotly-format-no-nulls stream field-b value-b)))


(defun plotly-generate-data/impl (stack geometrics index.table)
  (bind ((mapping (read-mapping geometrics))
         (data (data-layer stack))
         (table (cdr index.table))
         (aesthetics (read-aesthetics geometrics))
         (x (x mapping))
         (y (y mapping))
         (z (z mapping))
         (color (color mapping))
         (shape (shape mapping))
         (label (label mapping))
         (label-position (label-position aesthetics))
         (size (size mapping)))
    (macrolet ((set-name (axis)
                 (with-gensyms (!number !data !index)
                     `(unless (null ,axis)
                        (let ((,!number
                                (if (integerp ,axis)
                                    ,axis
                                    (~> data
                                        vellum.table:header
                                        (vellum.header:alias-to-index ,axis)))))
                          (if-let ((table-content (gethash ,!number table)))
                            (setf ,axis (car table-content))
                            (let ((,!data (plotly-extract-data data ,axis))
                                  (,!index (incf (car index.table))))
                              (unless (null ,!data)
                                (setf ,axis (format nil "v~a" ,!index)
                                      (gethash ,!number table)
                                      (cons ,axis (with-output-to-string (*json-stream*)
                                                    (value ,!data))))))))))))
      (set-name x)
      (set-name y)
      (set-name z)
      (set-name size)
      (set-name label)
      (set-name color))
    (values
     (with-output-to-string (stream)
       (json (stream)
         (object
           (slot "x" (var x))
           (slot "y" (var y))
           (unless (null z)
             (slot "z" (var z)))
           (slot "mode" (value (plotly-mode geometrics mapping)))
           (slot "type" (value (plotly-type geometrics)))
           (slot "name" (value (label aesthetics)))
           (slot "marker"
                 (object
                   #1=(cond
                        (color (slot "color" (var color)))
                        (aesthetics (slot "color" (value (color aesthetics)))))
                   (when size
                     (slot "size" (var size)))
                   (when label
                     (slot "text" (var label)))
                   (slot "textposition" (value label-position))))
           (slot "list" (object #1#))))))))


(defun plotly-generate-data (stack)
  (iterate
    (with geometrics = (geometrics-layers stack))
    (with index.table = (cons 0 (make-hash-table)))
    (for g in geometrics)
    (collect (plotly-generate-data/impl stack g index.table)
      into data-forms)
    (finally (return (values data-forms
                             (~> index.table
                                 cdr
                                 hash-table-values))))))


(defun plotly-format-axis (mapping axis)
  (when axis
    (object
      (slot "title"
            (object
              (cond ((and axis (label axis))
                     (plotly-format-no-nulls "text" (label axis)))
                    (mapping
                     (plotly-format-no-nulls "text" mapping)))))
      (slot "scaleanchor" (value (scale-anchor axis)))
      (slot "range" (value (range axis)))
      (slot "constrain" (value (constrain axis)))
      (slot "scaleratio" (value (scale-ratio axis)))
      (slot "ticklen" (value (tick-length axis)))
      (slot "dtick" (value (dtick axis))))))


(defun plotly-generate-layout (stack)
  (bind ((aesthetics (aesthetics-layer stack))
         (xaxis (x aesthetics))
         (yaxis (y aesthetics))
         (geometrics (geometrics-layers stack))
         (mapping (if (endp geometrics)
                      nil
                      (read-mapping (first geometrics)))))
    (with-output-to-string (stream)
      (json (stream)
        (object
          (slot "height" (value (height aesthetics)))
          (slot "width" (value (width aesthetics)))
          (slot "title" (value (label aesthetics)))
          (slot "xaxis"
                (plotly-format-axis (and mapping (x mapping))
                                    xaxis))
          (slot "yaxis"
                (plotly-format-axis (and mapping (y mapping))
                                    yaxis)))))))


(defun plotly-visualize (stack stream)
  (bind ((layout (plotly-generate-layout stack))
         ((:values data-forms variables) (plotly-generate-data stack)))
    (format stream "<html>~%")
    (format stream "<head>~%")
    (format stream "<script src='https://cdn.plot.ly/plotly-latest.min.js'></script></head>~%")
    (format stream "<body>")
    (format stream
            "<div id='plotDiv'><!-- Plotly chart will be drawn inside this DIV --></div>~%")
    (format stream "<script type='text/javascript'>~%")
    (iterate
      (for (name . value) in variables)
      (format stream "var ~a = ~a;~%~%" name value))
    (format stream "Plotly.newPlot('plotDiv', [~{~a~^,~}], ~a);~%"
            data-forms
            layout)
    (format stream "</script>~%")
    (format stream "</body>")
    (format stream "</html>~%")))
