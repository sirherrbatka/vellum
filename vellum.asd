(asdf:defsystem #:vellum
  :name "vellum"
  :version "1.3.1"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :description "Data Frames for Common Lisp"
  :depends-on ( #:iterate       #:serapeum
                #:lparallel     #:closer-mop
                #:metabang-bind #:alexandria
                #:agnostic-lizard
                (:version #:cl-data-structures ((>= "1.4.0")))
                #:documentation-utils-extensions)
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
                             (:file "types")
                             (:file "macros")
                             (:file "protocol")
                             (:file "utils")
                             (:file "functions")
                             (:file "conditions")
                             (:file "internal")
                             (:file "implementation")
                             (:file "documentation")))
               (:module "selection"
                :components ((:file "package")
                             (:file "implementation")))
               (:module "table"
                :components ((:file "package")
                             (:file "variables")
                             (:file "macros")
                             (:file "generics")
                             (:file "types")
                             (:file "conditions")
                             (:file "functions")
                             (:file "documentation")
                             (:file "to-table")
                             (:file "internal")
                             (:file "implementation")))
               (:module "api"
                :components ((:file "package")
                             (:file "macros")
                             (:file "conditions")
                             (:file "generics")
                             (:file "functions")))
               (:module "integration"
                :components ((:file "package")
                             (:file "cl-ds")))))
