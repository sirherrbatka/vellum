(cl:in-package #:vellum.selection)


(define-constant +bracket-forms+
    '((:take-from :skip-from)
      (:take-to :skip-to)
      (take-selection-block skip-selection-block))
  :test 'equal)


(defun openings ()
  (first +bracket-forms+))


(defun closings ()
  (second +bracket-forms+))


(defun block-classes ()
  (third +bracket-forms+))


(defun opening-p (symbol)
  (member symbol (openings)))


(defun closing-p (symbol)
  (member symbol (closings)))


(defun matching-opening-p (closing symbol)
  (eql (position symbol (openings))
       (position closing (closings))))


(defun matching-block-class (symbol)
  (or (when-let ((position (position symbol (openings))))
        (elt (block-classes) position))
      (when-let ((position (position symbol (closings))))
        (elt (block-classes) position))))


(defun matching-closing-p (opening symbol)
  (let ((opening-position (position opening (openings)))
        (closing-position (position symbol (closings))))
    (and opening-position closing-position
         (eql opening-position closing-position))))


(defun matching-opening (closing)
  (elt (openings) (position closing (closings))))
