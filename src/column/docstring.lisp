(in-package #:cl-df.column)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function column-type
    (:description "Type of the elements stored in the column."
     :arguments ((column "Data frame column."))))

  (function move-iterator
    (:description "Shifts ITERATOR forward on all columns."
     :exceptional-situations "Should signal condition if this operation would move beyond column bounds."))

  (function column-at
    (:description "Obtains value under INDEX from the COLUMN."
     :returns "Value under INDEX of the COLUMN. :NULL if location is empty."
     :exceptional-situations "Will signal INDEX-OUT-OF-COLUMN-BOUNDS if index is out of column bounds."))

  (function (setf column-at)
    (:description "Sets value under INDEX in the COLUMN."
     :exceptional-situations "Will signal INDEX-OUT-OF-COLUMN-BOUNDS if index is out of column bounds."))

  (function column-size
    (:description "The total size of the COLUMN.")))
