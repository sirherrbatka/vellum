(cl:in-package #:vellum.csv)


(define-condition wrong-number-of-columns-in-the-csv-file
    (cl-ds:file-releated-error)
  ())


(define-condition csv-format-error
    (cl-ds:file-releated-error)
  ())
