#!/bin/bash

sbcl --noinfom --dynamic-space-size 8000 --no-sysinit --no-userinit --load run-tests.lisp
