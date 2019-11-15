(in-package #:cl-user)

(defpackage :cl-data-frames.aux-package
  (:use #:cl)
  (:export #:rexport))

(in-package :cl-data-frames.aux-package)

(defmacro rexport (package &body symbols)
  (alexandria:with-gensyms (!package)
    `(let ((,!package (find-package ,package)))
       ,@(mapcar (lambda (symbol) `(import ',symbol ,!package))
                 symbols)
       ,@(mapcar (lambda (symbol) `(export ',symbol ,!package))
                 symbols))))

(rexport :cl-data-frames.aux-package
  alexandria:if-let
  alexandria:when-let
  alexandria:when-let*
  alexandria:cswitch
  alexandria:eswitch
  alexandria:switch
  alexandria:define-constant
  alexandria:alist-hash-table
  alexandria:copy-hash-table
  alexandria:hash-table-alist
  alexandria:hash-table-keys
  alexandria:hash-table-plist
  alexandria:hash-table-values
  alexandria:maphash-keys
  alexandria:maphash-values
  alexandria:plist-hash-table
  alexandria:compose
  alexandria:curry
  alexandria:ensure-function
  alexandria:ensure-functionf
  alexandria:multiple-value-compose
  alexandria:rcurry
  alexandria:alist-plist
  alexandria:appendf
  alexandria:nconcf
  alexandria:reversef
  alexandria:nreversef
  alexandria:circular-list
  alexandria:circular-list-p
  alexandria:circular-tree-p
  alexandria:doplist
  alexandria:ensure-car
  alexandria:ensure-cons
  alexandria:ensure-list
  alexandria:flatten
  alexandria:lastcar
  alexandria:make-circular-list
  alexandria:map-product
  alexandria:mappend
  alexandria:nunionf
  alexandria:plist-alist
  alexandria:proper-list
  alexandria:proper-list-length
  alexandria:proper-list-p
  alexandria:remove-from-plist
  alexandria:remove-from-plistf
  alexandria:delete-from-plist
  alexandria:delete-from-plistf
  alexandria:set-equal
  alexandria:setp
  alexandria:unionf
  alexandria:clamp
  alexandria:iota
  alexandria:lerp
  alexandria:map-iota
  alexandria:maxf
  alexandria:mean
  alexandria:median
  alexandria:minf
  alexandria:subfactorial
  alexandria:array-index
  alexandria:array-length
  alexandria:copy-array
  alexandria:copy-sequence
  alexandria:deletef
  alexandria:emptyp
  alexandria:ends-with
  alexandria:ends-with-subseq
  alexandria:extremum
  alexandria:first-elt
  alexandria:last-elt
  alexandria:length=
  alexandria:map-combinations
  alexandria:map-derangements
  alexandria:map-permutations
  alexandria:proper-sequence
  alexandria:random-elt
  alexandria:removef
  alexandria:rotate
  alexandria:sequence-of-length-p
  alexandria:shuffle
  alexandria:starts-with
  alexandria:starts-with-subseq
  alexandria:once-only
  alexandria:parse-body
  alexandria:parse-ordinary-lambda-list
  alexandria:with-gensyms
  alexandria:with-unique-names
  alexandria:ensure-symbol
  alexandria:format-symbol
  alexandria:make-gensym
  alexandria:make-gensym-list
  alexandria:make-keyword
  alexandria:string-designator
  alexandria:negative-double-float
  alexandria:negative-fixnum-p
  alexandria:negative-float
  alexandria:negative-float-p
  alexandria:negative-long-float
  alexandria:negative-long-float-p
  alexandria:negative-rational
  alexandria:negative-rational-p
  alexandria:negative-real
  alexandria:negative-single-float-p
  alexandria:non-negative-double-float
  alexandria:non-negative-double-float-p
  alexandria:non-negative-fixnum
  alexandria:non-negative-fixnum-p
  alexandria:non-negative-float
  alexandria:non-negative-float-p
  alexandria:non-negative-integer-p
  alexandria:non-negative-long-float
  alexandria:non-negative-rational
  alexandria:non-negative-real-p
  alexandria:non-negative-short-float-p
  alexandria:non-negative-single-float
  alexandria:non-negative-single-float-p
  alexandria:non-positive-double-float
  alexandria:non-positive-double-float-p
  alexandria:non-positive-fixnum
  alexandria:non-positive-fixnum-p
  alexandria:non-positive-float
  alexandria:non-positive-float-p
  alexandria:non-positive-integer
  alexandria:non-positive-rational
  alexandria:non-positive-real
  alexandria:non-positive-real-p
  alexandria:non-positive-short-float
  alexandria:non-positive-short-float-p
  alexandria:non-positive-single-float-p
  alexandria:positive-double-float
  alexandria:positive-double-float-p
  alexandria:positive-fixnum
  alexandria:positive-fixnum-p
  alexandria:positive-float
  alexandria:positive-float-p
  alexandria:positive-integer
  alexandria:positive-rational
  alexandria:positive-real
  alexandria:positive-real-p
  alexandria:positive-short-float
  alexandria:positive-short-float-p
  alexandria:positive-single-float
  alexandria:positive-single-float-p
  alexandria:coercef
  alexandria:negative-double-float-p
  alexandria:negative-fixnum
  alexandria:negative-integer
  alexandria:negative-integer-p
  alexandria:negative-real-p
  alexandria:negative-short-float
  alexandria:negative-short-float-p
  alexandria:negative-single-float
  alexandria:non-negative-integer
  alexandria:non-negative-long-float-p
  alexandria:non-negative-rational-p
  alexandria:non-negative-real
  alexandria:non-negative-short-float
  alexandria:non-positive-integer-p
  alexandria:non-positive-long-float
  alexandria:non-positive-long-float-p
  alexandria:non-positive-rational-p
  alexandria:non-positive-single-float
  alexandria:of-type
  alexandria:positive-integer-p
  alexandria:positive-long-float
  alexandria:positive-long-float-p
  alexandria:positive-rational-p
  alexandria:type=
  alexandria:required-argument
  alexandria:ignore-some-conditions
  alexandria:simple-style-warning
  alexandria:simple-reader-error
  alexandria:simple-parse-error
  alexandria:simple-program-error
  alexandria:unwind-protect-case
  alexandria:featurep
  alexandria:with-input-from-file
  alexandria:with-output-to-file
  alexandria:read-stream-content-into-string
  alexandria:read-file-into-string
  alexandria:write-string-into-file
  alexandria:read-stream-content-into-byte-vector
  alexandria:read-file-into-byte-vector
  alexandria:write-byte-vector-into-file
  alexandria:copy-stream
  alexandria:copy-file
  alexandria:symbolicate
  alexandria:assoc-value
  alexandria:rassoc-value
  alexandria:destructuring-case
  alexandria:destructuring-ccase
  alexandria:destructuring-ecase

  serapeum:~>
  serapeum:->
  serapeum:~>>
  serapeum:batches
  serapeum:supertypep
  serapeum:tuple
  serapeum:runs
  serapeum:lret
  serapeum:lret*
  serapeum:no
  serapeum:nor
  serapeum:nand
  serapeum:econd
  serapeum:ordering
  serapeum:nest
  serapeum:nlet
  serapeum:flip
  serapeum:fork
  serapeum:fork2
  serapeum:hook
  serapeum:hook2
  serapeum:juxt
  serapeum:dict
  serapeum:dict*
  serapeum:box
  serapeum:unbox
  serapeum:vect
  serapeum:parse-number
  serapeum:parse-float
  serapeum:ensure
  serapeum:random-in-range
  serapeum:finc
  serapeum:make
  serapeum:vector=
  serapeum:fbind
  serapeum:take
  serapeum:drop
  serapeum:take-while
  serapeum:drop-while
  serapeum:extrema
  serapeum:eval-always
  serapeum:plist-keys
  serapeum:plist-values
  serapeum:def
  serapeum:round-to
  serapeum:assure
  serapeum:trim-whitespace

  metabang.bind:bind

  iterate:iterate
  iterate:iter
  iterate:display-iterate-clauses
  iterate:defsynonym
  iterate:dsetq
  iterate:declare-variables
  iterate:defmacro-clause
  iterate:defmacro-driver
  iterate:defclause-sequence
  iterate:initially
  iterate:after-each
  iterate:finally
  iterate:finally-protected
  iterate:else
  iterate:if-first-time
  iterate:first-iteration-p
  iterate:first-time-p
  iterate:finish
  iterate:leave
  iterate:next-iteration
  iterate:next
  iterate:terminate
  iterate:repeat
  iterate:for
  iterate:counting
  iterate:as
  iterate:generate
  iterate:generating
  iterate:in
  iterate:sum
  iterate:summing
  iterate:multiply
  iterate:multiplying
  iterate:maximize
  iterate:minimize
  iterate:maximizing
  iterate:minimizing
  iterate:always
  iterate:never
  iterate:thereis
  iterate:finding
  iterate:collect
  iterate:collecting
  iterate:with
  iterate:while
  iterate:until
  iterate:adjoining
  iterate:nconcing
  iterate:appending
  iterate:nunioning
  iterate:unioning
  iterate:reducing
  iterate:accumulate
  iterate:accumulating)
