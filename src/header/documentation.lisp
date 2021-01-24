(cl:in-package #:vellum.header)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (type
   no-column
   (:description "Error signalled when trying to reference non existing column in the current header."))

  (type
   no-header
   (:description "Error signalled when trying to reference current header, but it is not binded."))

  (type
   no-row
   (:description "Error signalled when trying to reference current row, but it is not binded."))

  (type
   predicate-failed
   (:description "Error signalled when trying to set the column value in row to somethining rejected by the predicate."))

  (function
   check-predicate
   (:description "Calls the column predicate on the value "
    :exceptional-situations "Signals NO-COLUMN when the COLUMN is not present in the current HEADER. Signals PREDICAT-FAILED when the predicate function returns nil."))

  (function
   ensure-index
   (:description "Converts strings and symbols to index of column in the HEADER. If a numeric index is passed it is returned unaltered."
    :exceptional-situations "Will signal NO-COLUMN if no column in the HEADER with such INDEX/NAME exists."))

  (function
   set-row
   (:description "Sets the inner value of *CURRENT-ROW* to the passed ROW."
    :exceptional-situations "Will signal NO-ROW when *CURRENT-ROW* is unbound."))

  (function
   with-header
   (:description "Expands to form binding *HEADER* and establishing *CURRENT-ROW*."
    :notes "Call SET-ROW to actually set the value of the row to a specific object."))

  (function
   bind-row-closure
   (:description "Prepares passed function/nil/bind-row instance to be used. It will return functions unaltered, header optimized closure from the bind-row instance and constantly nil closure when nil is passed."))

  (function
   column-count
   (:description "How many columns header specifies?"))

  (function
   index-to-name
   (:description "Converts a numeric INDEX designating column to the column name in the HEADER."
    :exceptional-situations "Signalls NO-COLUMN when the INDEX is not found in the HEADER."))

  (function
   name-to-index
   (:description "Converts a column NAME (which is either string or symbol) to the numeric index designating column in the HAEDER."
    :exceptional-situations "Signalls NO-COLUMN when the NAME is not found in the HEADER."))

  (type
   name-duplicated
   (:description "Names in the header are supposed to be unique. If an operation is performed that would violate that rule, this error is signalled."))

  (function
   header
   (:description "Returns the current active header."
    :exceptional-situations "Will signal NO-HEADER when there is no active header."))

  (function
   validate-active-header
   (:description "Signals NO-HEADER error when the *HEADER* is NIL."))

  (function
   rr
   (:description "Extracts value from the current row."
    :exceptional-situations "Signals NO-COLUMN, NO-HEADER, NO-ROW exception if: column is not found in the current header, there is no active header, there is no active row."))

  (function
   brr
   (:description "Expands to a closure extracting COLUMN from the CURRENT-ROW if only one argument has been passed to the macro, or expands to a closure extracting multiple values packed into a list, elementwise."
    :exceptional-situations "Expanded closure signals NO-COLUMN, NO-HEADER, NO-ROW exception if: column is not found in the current header, there is no active header, there is no active row."
    :notes "Expanded closure is not thread safe.")))
