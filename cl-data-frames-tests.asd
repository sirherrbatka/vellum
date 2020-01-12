(cl:in-package #:cl-user)


(asdf:defsystem cl-data-frames
  :name "cl-data-frames"
  :version "0.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on (:prove :cl-data-frames)
  :serial T
  :pathname "src"
  :components ((:module "column"
                :components ((:test-file "tests")))
               (:module "table"
                :components ((:test-file "tests")))
               (:module "csv"
                :components ((:test-file "parser-tests")))))
