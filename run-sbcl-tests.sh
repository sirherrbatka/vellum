#!/bin/bash

sbcl --dynamic-space-size 8000 --noinform --no-sysinit --no-userinit --load ./run-tests.lisp
