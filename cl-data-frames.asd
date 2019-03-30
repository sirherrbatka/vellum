(in-package #:cl-user)


(asdf:defsystem cl-data-frames
  :name "cl-data-frames"
  :version "0.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on ( :iterate      :serapeum
                :prove        :cl-data-structures
                :lparallel    :metabang-bind
                :cl-csv       :alexandria
                :documentation-utils-extensions
                :gzip-stream)
  :defsystem-depends-on (:prove-asdf)
  :serial T
  :pathname "src"
  :components ((:file "aux-package")
               (:module "column"
                :components ((:file "package")
                             (:file "protocol")
                             (:file "types")
                             (:file "conditions")
                             (:file "docstring")
                             (:file "internal")
                             (:file "implementation")
                             (:test-file "tests")))
               (:module "header"
                :components ((:file "package")
                             (:file "variables")
                             (:file "macros")
                             (:file "protocol")
                             (:file "functions")
                             (:file "types")
                             (:file "conditions")
                             (:file "internal")
                             (:file "implementation")))
               (:module "table"
                :components ((:file "package")
                             (:file "variables")
                             (:file "macros")
                             (:file "generics")
                             (:file "types")
                             (:file "conditions")
                             (:file "functions")
                             (:file "to-table")
                             (:file "implementation")
                             (:test-file "tests")))
               (:module "api"
                :components ((:file "package")
                             (:file "conditions")
                             (:file "generics")))
               (:module "csv"
                :components ((:file "package")
                             (:file "implementation")))))
