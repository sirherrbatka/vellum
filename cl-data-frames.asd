(cl:in-package #:cl-user)


(asdf:defsystem cl-data-frames
  :name "cl-data-frames"
  :version "0.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on ( :iterate       :serapeum
                :lparallel     :cl-data-structures
                :metabang-bind :alexandria
                :local-time    :postmodern
                :cl-postgres   :s-sql
                :mcclim        :clim
                :documentation-utils-extensions)
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
                             (:file "implementation")))
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
                             (:file "internal")
                             (:file "implementation")))
               (:module "api"
                :components ((:file "package")
                             (:file "conditions")
                             (:file "generics")
                             (:file "functions")))
               (:module "integration"
                :components ((:file "package")
                             (:file "cl-ds")
                             (:file "postmodern")
                             (:file "clim")))
               (:module "csv"
                :components ((:file "package")
                             (:file "conditions")
                             (:file "common")
                             (:file "generics")
                             (:file "parser")
                             (:file "implementation")))))
