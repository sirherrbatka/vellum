(in-package #:cl-df.column)


(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function column-type
    (:description "Type of the elements stored in the column."
     :arguments ((column "Data frame column."))))

  (function move-iterator
    (:description "Shifts ITERATOR forward on all columns."
     :exceptional-situations "Should signal condition if this operation would move beyond column bounds."))

  (function replica
    (:description "Creates new column with the same content as the old one. Changes in the new istance won't effect the COLUMN. Changes in the COLUMN may effect the new instance if ISOLATE is false and will not effect new instance if ISOLATE is true."))

  (function column-at
    (:description "Obtains value under INDEX from the COLUMN."
     :returns "Value under INDEX of the COLUMN. :NULL if location is empty."
     :exceptional-situations ("Will signal INDEX-OUT-OF-COLUMN-BOUNDS if INDEX is out of column bounds."
                              "Will signal type-error if INDEX is not INTEGER.")))

  (function (setf column-at)
    (:description "Sets value under INDEX in the COLUMN."
     :exceptional-situations ("Will signal INDEX-OUT-OF-COLUMN-BOUNDS if INDEX is out of column bounds."
                              "Will signal type-error if INDEX is not INTEGER.")))

  (function (setf column-size)
    (:description "Set size of the COLUMN."))

  (function column-size
    (:description "The total size of the COLUMN.")))
