#|
Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1) Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2) Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
|#

(asdf:defsystem #:vellum
  :name "vellum"
  :version "1.3.4"
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
                             (:file "functions")
                             (:file "documentation")))
               (:module "integration"
                :components ((:file "package")
                             (:file "cl-ds")))))
