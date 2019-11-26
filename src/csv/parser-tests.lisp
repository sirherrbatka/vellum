(in-package #:cl-df.csv)

(prove:plan 5)

(let ((output (map-into (make-array 12)
                        (curry #'make-array 0
                               :element-type 'character
                               :fill-pointer 0
                               :adjustable t))))
  (parse-csv-line #\, #\\ #\"
                  "\"7577363390200\",112276,42,0,\"F\",\"36093\",:IP,\"6-12-2017\",\"6-12-2017\",\"\",\"7577363390200,7577363390300,7577363391400,7577363390500,7577363391200,7577363390400,7577363390900,7577363390100,7577363390800,7577363391300,7577363391500,7577363391000,7577363391100,7577363390700,7577363390600\",\"K2970,E876,D72829,I10,G809,F419,E119,J45909,N83202\""
                  output
                  "N/A")
  (print output)
  (prove:ok (vector= output
                     #("7577363390200" "112276" "42" "0" "F" "36093" ":IP" "6-12-2017" "6-12-2017"
                       ""
                       "7577363390200,7577363390300,7577363391400,7577363390500,7577363391200,7577363390400,7577363390900,7577363390100,7577363390800,7577363391300,7577363391500,7577363391000,7577363391100,7577363390700,7577363390600"
                       "K2970,E876,D72829,I10,G809,F419,E119,J45909,N83202")
                     :test #'string=)))

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

  (cl-ds.utils:transform fill-pointer-to-zero output)
  (parse-csv-line #\, #\\ #\"
                  "test,1,test2,test3"
                  output
                  "N/A"
                  (lambda (field index)
                    (if (= index 1)
                        (string= "1" field)
                        t)))
  (prove:ok (vector= output #("test" "1" "test2,test3")
                     :test #'string=)))

(prove:finalize)
