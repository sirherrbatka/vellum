(cl:in-package #:vellum.header)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function
   brr
   (:description "Expands to closure extracting COLUMN from the CURRENT-ROW."
    :exceptional-situations "Expanded closure signals NO-COLUMN, NO-HEADER, NO-ROW exception if: column is not found in the current header, there is no active header, there is no active row."
    :notes "Expanded closure is not thread safe.")))
