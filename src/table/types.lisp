(in-package #:cl-df.table)


(defclass fundamental-table (cl-ds:traversable)
  ())


(defclass standard-table (fundamental-table)
  ((%header :reader header
            :initarg :header)
   (%columns :reader read-columns
             :writer write-columns
             :initarg :columns
             :type vector)))


(defmethod cl-ds.utils:cloning-information append ((table standard-table))
  '((:header header)
    (:columns read-columns)))


(defclass table-row ()
  ((%iterator :initarg :iterator
              :reader read-iterator)))


(defclass setfable-table-row (table-row)
  ())


(defclass selection (cl-ds:traversable)
  ((%starts :initarg :starts
           :reader read-starts)
   (%ends :initarg :ends
          :reader read-ends)))


(defun selection (start end &rest more-start-ends)
  (declare (type non-negative-fixnum start end))
  (let* ((arguments (~> `(,start ,end ,@more-start-ends) (batches 2)))
         (starts (map '(vector fixnum) #'first arguments))
         (ends (map '(vector fixnum) #'second arguments)))
    (unless (every (compose (curry #'eql 2) #'length) arguments)
      (error 'cl-ds:argument-error
             :argument 'more-start-ends
             :format-control "Odd number of arguments passed to the selection (~a)."
             :format-arguments (length more-start-ends)))
    (make 'selection :starts starts :ends ends)))


(defclass standard-table-range (cl-ds:fundamental-forward-range)
  ((%table-row :initarg :table-row
               :reader read-table-row)
   (%header :initarg :header
            :reader read-header)
   (%row-count :initarg :row-count
               :type fixnum
               :reader read-row-count)))


(defmethod cl-ds.utils:cloning-information append
    ((range standard-table-range))
  '((:table-row read-table-row)
    (:header read-header)
    (:row-count read-row-count)))


(defmethod cl-ds.utils:cloning-information append
    ((range selection))
  `((:starts read-starts)
    (:ends read-ends)))
