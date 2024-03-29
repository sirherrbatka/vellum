(cl:in-package #:vellum.selection)


(defclass content ()
  ())


(defclass between (content)
  ((%from :initarg :from
          :reader read-from)
   (%to :initarg :to
        :reader read-to))
  (:default-initargs
   :from nil
   :to nil))


(defun between (&key from to)
  (make 'between
         :from from
         :to to))


(defgeneric content (selection translate current-position limit))


(defgeneric address-range (selector/sequence translate limit))


(defclass selector ()
  ((%callback :initarg :callback
              :reader read-callback)))


(defmethod content ((selection between) translate
                    current-position limit)
  (let ((from (funcall translate
                       (or (read-from selection)
                           current-position)))
        (to (if-let ((to (read-to selection)))
              (funcall translate to)
              limit)))
    (cl-ds:iota-range :from from
                      :to to
                      :by (if (<= from to) 1 -1))))


(defun s (&rest forms)
  (make
   'selector
   :callback (lambda (translate limit &aux (position -1))
               (~> forms
                   (cl-ds.alg:multiplex
                    :function (lambda (x)
                                (typecase x
                                  (cl-ds:traversable x)
                                  (vector x)
                                  (list (bind (((first . rest) x))
                                          (if (listp rest)
                                              (vector x)
                                              (content (between :from first
                                                                :to (or rest limit))
                                                       translate
                                                       (1+ position)
                                                       limit))))
                                  (content (content x translate (1+ position) limit))
                                  (atom (list x)))))
                   (cl-ds.alg:on-each (lambda (x)
                                        (setf position (funcall translate x))
                                        x))))))


(defun rs (&rest forms)
  (make
   'selector
   :callback (lambda (translate limit &aux (position (1- limit)))
               (~> forms
                   (cl-ds.alg:multiplex
                    :key (lambda (x)
                           (typecase x
                             (cl-ds:traversable x)
                             (vector x)
                             (list (bind (((first . rest) x))
                                     (if (listp rest)
                                         x
                                         (content (between :from first
                                                           :to rest)
                                                  translate
                                                  (1- position)
                                                  limit))))
                             (content (content x translate (1- position) limit))
                             (atom (list x)))))
                   (cl-ds.alg:on-each (lambda (x)
                                        (setf position
                                              (funcall translate x))
                                        x))))))


(defun s-list (header &rest forms)
  (~> (apply #'s forms)
      (vellum.selection:address-range
       (lambda (spec)
         (vellum.header:ensure-index header
                                     (if (listp spec)
                                         (first spec)
                                         spec)))
       (vellum.header:column-count header))
      cl-ds.alg:to-list))


(define-condition name-when-selecting-row (cl-ds:invalid-value)
  ())


(defmethod address-range ((selector/sequence selector) translate limit)
  (funcall (read-callback selector/sequence)
           translate
           limit))


(defmethod address-range ((selector/sequence sequence) translate limit)
  (cl-ds.alg:on-each selector/sequence (lambda (x) (funcall translate x) x)))
