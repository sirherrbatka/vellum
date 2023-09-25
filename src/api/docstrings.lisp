(cl:in-package #:vellum)

(docs:define-docs
  :formatter docs.ext:rich-aggregating-formatter

  (function find-row
            (:description "Attempts to find row."
             :returns "Selected columns values (all by default) as a list. T as secondary value if row was found, NIL otherwise."
             :examples ("(prove:is (vellum:find-row (vellum:to-table '((1 2 3) (3 4 5)) :columns '(a b c)) (vellum:bind-row (c) (when (= c 5) (vellum:found-row)))) '(3 4 5))"
                        "(prove:is (vellum:find-row (vellum:to-table '((1 2 3) (3 4 5)) :columns '(a b c)) (vellum:bind-row (c) (when (= c 5) (vellum:found-row a)))) '(3))")
             :see-also (found-row)))

  (function column-list
            (:description "Returns selected column values in the current row as a list."))

  (function rename-columns
            (:description "Creates new frame from the old one with altered column names."))

  (macro found-row
         (:description "Evaluation of this macro expansion will inform find-row that row was found"
          :see-also (find-row))))
