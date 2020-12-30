(cl:in-package #:cl-user)


(asdf:defsystem vellum-tests
  :name "vellum tests"
  :version "0.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on (:prove :vellum)
  :defsystem-depends-on (:prove-asdf)
  :serial T
  :pathname "src"
  :components ((:module "column"
                :components ((:test-file "tests")))
               (:module "table"
                :components ((:test-file "tests")))
               (:module "csv"
                :components ((:test-file "parser-tests")))))
