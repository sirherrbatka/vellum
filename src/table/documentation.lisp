(cl:in-package #:vellum.table)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function
   hstack*
   (:description "Concatenates multiple frames row-wise."
    :arguments ((frame "A data frame. Columns from this data frame will be placed as first.")
                (more-frames "Traversable object presenting more of the data frames."))))

  (function
   select
   (:description "Select subset of COLUMNS and ROWS from the FRAME."))

  (function
   transformation-error
   (:description "Error signalled when transformation of rows fails for some reason."
    :notes "The cause of the error can be extracted with MORE-CONDITIONS:CAUSE."))

  (function
   vstack*
   (:description "Concatenates multiple frames column-wise."))

  (function
   nullify
   (:description "Should be called from dynamic environment established in the callback passed to the TRANSFORM. Deletes all values in the current row, leaving :NULLs."))

  (function
   drop-row
    (:description "Should be called from dynamic environment established in the callback passed to the TRANSFORM. When called, removes row from the data-frame and pefrorms non-local exit."))

  (function
    rr
    (:description "Extracts value from the current row."
     :exceptional-situations "Signals NO-COLUMN, NO-HEADER, NO-ROW exception if: column is not found in the current header, there is no active header, there is no active row."))

  (function
    make-bind-row
    (:description "Conrstucts the BIND-ROW instance. This function is typically not called explicitly but instead part of the BIND-ROW macros macroexpansion."))

  (function
    brr
    (:description "Expands to a closure extracting COLUMN from the CURRENT-ROW if only one argument has been passed to the macro, or expands to a closure extracting multiple values packed into a list, elementwise."
     :exceptional-situations "Expanded closure signals NO-COLUMN, NO-HEADER, NO-ROW exception if: column is not found in the current header, there is no active header, there is no active row."
     :notes "Expanded closure is not thread safe.")))
