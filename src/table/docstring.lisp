(cl:in-package #:vellum.table)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function
   hstack*
   (:description "Concatenates multiple frames row-wise."
    :arguments ((frame "A data frame. Columns from this data frame will be placed as first.")
                (more-frames "Traversable object presenting more of the data frames."))))

  (function
   nullify
   (:description "Should be called from dynamic environment established in the callback passed to the TRANSFORM. Deletes all values in the current row, leaving :NULLs."))

  (function
   drop-row
   (:description "Should be called from dynamic environment established in the callback passed to the TRANSFORM. When called, removes row from the data-frame and pefrorms non-local exit.")))
