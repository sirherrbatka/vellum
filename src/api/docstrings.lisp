(cl:in-package #:vellum)

(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function find-row
            (:description "Attempts to find row."
             :returns "Selected columns values (all by default) as a list. T as secondary value if row was found, NIL otherwise."
             :examples ("(prove:is (vellum:find-row (vellum:to-table '((1 2 3) (3 4 5)) :columns '(a b c)) (vellum:bind-row (c) (when (= c 5) (vellum:found-row)))) '(3 4 5))"
                        "(prove:is (vellum:find-row (vellum:to-table '((1 2 3) (3 4 5)) :columns '(a b c)) (vellum:bind-row (c) (when (= c 5) (vellum:found-row a)))) '(3))")
             :see-also (found-row)))

  (function add-columns
            (:description "Creates new frame from the old one by adding additional columns."
             :arguments ((frame "Base data frame")
                         (column-specs "Column-specs, as in :COLUMNS in MAKE-TABLE"))))

  (function join
            (:description "Performs METHOD (:LEFT or :INNER) join using ALGORITHM (:HASH) on FRAME-SPECS list. Each element of FRAME-SPECS list contains elements in the following order label for data-frame, data-frame and description of columns used as join key. Each description is either name of the column, number of the column; or two elements list consisting of name or number of the column and function designator used as key."
             :examples "(let* ((frame-1 (vellum:transform (vellum:make-table :columns '(a b))
                           (vellum:bind-row (a b)
                             (setf a vellum.table:*current-row*)
                             (setf b (format nil \"a~a\" a)))
                             :end 5))
         (frame-2 (vellum:transform (vellum:make-table :columns '(a b))
                             (vellum:bind-row (a b)
                               (setf a vellum.table:*current-row*)
                               (setf b (format nil \"b~a\" a)))
                             :end 5))
         (result (vellum:join :hash :inner
                              `((:frame-1 ,frame-1 a)
                                (:frame-2 ,frame-2 a)))))
    (vellum:show :text result))"))

  (function order-by
            (:description "Builds new data-frame by sorting input TABLE data frame. Multiple columns and comparators can be used."
             :notes ("This function is calls CL:SORT to order data according to first column, and calls STABLE-SORT for all other columns.")))

  (function column-list
            (:description "Returns selected column values in the current row as a list."))

  (function rename-columns
            (:description "Creates new frame from the old one with altered column names."))

  (function found-row
         (:description "Evaluation of this macro expansion will inform find-row that row was found"
          :see-also (find-row))))
