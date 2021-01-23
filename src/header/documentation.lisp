(cl:in-package #:vellum.header)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function
   rr
   (:description "Extracts value from the current row."
    :exceptional-situations "Signals NO-COLUMN, NO-HEADER, NO-ROW exception if: column is not found in the current header, there is no active header, there is no active row."))

  (function
   brr
   (:description "Expands to a closure extracting COLUMN from the CURRENT-ROW if only one argument has been passed to the macro, or expands to a closure extracting multiple values packed into a list, elementwise."
    :exceptional-situations "Expanded closure signals NO-COLUMN, NO-HEADER, NO-ROW exception if: column is not found in the current header, there is no active header, there is no active row."
    :notes "Expanded closure is not thread safe.")))
