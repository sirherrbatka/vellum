(let ((quicklisp-init (merge-pathnames "quicklisp/setup.lisp"
                                       (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
    (load quicklisp-init)))

(quicklisp:quickload :vellum-tests :silent t)

(setf prove:*enable-colors* nil)

(unwind-protect
     (let ((*error-output* (make-broadcast-stream)))
       (handler-bind
           ((lparallel.kernel:no-kernel-error
              (lambda (c)
                (declare (ignore c))
                (invoke-restart 'lparallel.kernel:make-kernel 4))))
         (prove:run :vellum/tests)))
  (cl-user::quit))
