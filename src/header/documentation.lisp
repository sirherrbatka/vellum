(cl:in-package #:vellum.header)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (type
   fundamental-header
   (:description "Fundamental super-class of all headers."))

  (type
   column-signature
   (:description "Stores informations regarding the the specific column: type, name, predicate"))

  (type
   standard-header
   (:description "Default class of header. Supplies the default implementaiton of the header protocol."))

  (type
   frame-range-mixin
   (:description "Mixin class that can be inherited by various subclasses of CL-DS:FUNDAMENTAL-FORWARD-RANGE to provide partial support for the data-frames. This includes consume-front, peek-front, traverse, and accross methods. See vellum-csv system to see the example of the use case for this class."))

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
   read-new-value
   (:description "This function is used only in order to provide interactive restarts."))

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
   column-predicate
   (:description "What is the predicate That must be fullfilled by the values stored in the COLUMN?"
    :exceptional-situations "Will signal NO-COLUMN if no COLUMN in the HEADER."))

  (function
   column-signature
   (:description "Returns the complete COLUMN-SIGNATURE object from the HEADER for the specified COLUMN."
    :exceptional-situations "Will signal NO-COLUMN if no COLUMN in the HEADER."))

  (function
   make-row
   (:description "Constructs new row validated for the HEADERs predicates from the DATA."
    :notes "Since there is no column at this level, you may expect that the COLUMN-TYPE-ERROR can't be signalled. This is not true, as this function is used to implement the data-frame behavior for various streams. Examples of this is vellum-csv which depends on the whole base vellum package as a whole."))

  (function
   make-header
   (:description "Construct the header instance of CLASS."))

  (function
   column-type
   (:description "What is the type stored inside the COLUMN?"
    :exceptional-situations "Will signal NO-COLUMN if no COLUMN in the HEADER."))

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
    (:description "Signals NO-HEADER error when the *HEADER* is NIL.")))
