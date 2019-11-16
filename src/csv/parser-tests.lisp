(in-package #:cl-df.csv)

(prove:plan 3)

(let ((output (map-into (make-array 3)
                        (curry #'make-array 0
                               :element-type 'character
                               :fill-pointer 0
                               :adjustable t)))
      (fill-pointer-to-zero (lambda (x)
                              (setf (fill-pointer x) 0)
                              x)))
  (parse-csv-line #\, #\\ #\"
                  "test1,test2,test3"
                  output
                  "N/A")
  (prove:ok (vector= output #("test1" "test2" "test3")
                     :test #'string=))
  (cl-ds.utils:transform fill-pointer-to-zero output)
  (parse-csv-line #\, #\\ #\"
                  "\"test1\",test2,test3"
                  output
                  "N/A")
  (prove:ok (vector= output #("test1" "test2" "test3")
                     :test #'string=))
  (cl-ds.utils:transform fill-pointer-to-zero output)
  (parse-csv-line #\, #\\ #\"
                  "\"test,1\",test2,test3"
                  output
                  "N/A")
  (prove:ok (vector= output #("test,1" "test2" "test3")
                     :test #'string=))

  )

(prove:finalize)
