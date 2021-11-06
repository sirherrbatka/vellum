(cl:defpackage #:group-by-example
  (:use #:cl))


(cl:in-package #:group-by-example)


(defparameter *table*
  (vellum:to-table `((:even :another-key ,@(alexandria:iota 3))
                     (:odd :different-key ,@(alexandria:iota 3 :start 5)))
                   :columns '(group another-key nil nil nil)))

(vellum:show :text *table*)
#|
GROUP  ANOTHER-KEY    2  3  4
=============================
EVEN   ANOTHER-KEY    0  1  2
ODD    DIFFERENT-KEY  5  6  7
|#

(defparameter *grouped-and-filtered*
  (vellum:pipeline (*table*)
    (cl-ds.alg:group-by :key (vellum:brr group))
    (cl-ds.alg:group-by :key (vellum:brr another-key))
    (cl-ds.alg:on-each (vellum:brr 2 3 4))
    cl-ds.alg:multiplex
    (cl-ds.alg:only (lambda (number &aux (group (vellum:rr 'group)))
                      (alexandria:switch (group)
                        (:even (evenp number))
                        (:odd (oddp number)))))
    (cl-ds.alg:to-list)
    (vellum:to-table :columns '(group another-key number))))

(vellum:show :text *grouped-and-filtered*)
#|
GROUP  ANOTHER-KEY    NUMBER
============================
ODD    DIFFERENT-KEY  (5 7)
EVEN   ANOTHER-KEY    (0 2)
|#
