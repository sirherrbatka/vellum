(in-package #:cl-data-frames)


(define-condition row-cant-be-created
    (more-conditions:chainable-condition)
  ())


(define-condition file-input-row-cant-be-created
    (row-cant-be-created
     cl-ds:file-releated-error)
  ())
