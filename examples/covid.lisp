#|
This example demonstrates some basic data frame manipulation allowed by the vellum library.
Comments contain detailed explanation for each step.

First, let us load vellum and vellum-csv systems.
|#
(ql:quickload '(:vellum :vellum-csv :vellum-plot :local-time))


#|
This CSV file has quite a lot of columns and vellum requires you to list it all before reading in CSV file. Data is the summary of the 2020/2021 global covid pandemic in the state of the California.

Vellum-CSV knows how to read in certain data types, for instance integers. It will do so only if the :type option in column is specified and not T. However, there is no support for dates. No big deal, we can provide :body argument with bind-row form that will transform data while it is being stored in the data frame.

In this exapmle we are going to transform dates string representation into ever useful local-time dates. Local-time is the go-to CL library for this kind of job.
|#
(defparameter *source-data*
  (vellum:copy-from :csv
                    (asdf:system-relative-pathname :vellum "examples/california-history.csv")
                    :columns '(date ;; see body bind-row
                               state ;; data contains results from just california
                               (:name death :type integer)
                               (:name death-confirmed :type integer)
                               (:name death-increase :type integer)
                               death-probable
                               (:name hospitalized :type integer)
                               (:name hospitalized-cumulative :type integer)
                               (:name hospitalized-currently :type integer)
                               (:name hospitalized-increase :type integer)
                               (:name in-icu-cumulative :type integer)
                               (:name in-icu-currently :type integer)
                               (:name negative :type integer)
                               (:name negative-increase :type integer)
                               (:name negative-tests-antibody :type integer)
                               (:name negative-tests-people-antibody :type integer)
                               (:name negative-tests-viral :type integer)
                               (:name on-ventilator-cumulative :type integer)
                               (:name on-ventilator-currently :type integer)
                               (:name positive :type integer)
                               (:name positive-cases-viral :type integer)
                               (:name positive-increase :type integer)
                               (:name positive-score :type integer)
                               (:name positive-tests-antibody :type integer)
                               (:name positive-tests-antigen :type integer)
                               (:name positive-tests-people-antibody :type integer)
                               (:name positive-tests-people-antigen :type integer)
                               (:name positive-tests-viral :type integer)
                               (:name recovered :type integer)
                               (:name total-test-encounters-viral :type integer)
                               (:name total-test-encounters-viral-increase :type integer)
                               (:name total-test-results :type integer)
                               (:name total-test-results-increase :type integer)
                               (:name total-tests-antibody :type integer)
                               (:name total-tests-antigen :type integer)
                               (:name total-tests-people-antibody :type integer)
                               (:name total-tests-people-antigen :type integer)
                               (:name total-tests-people-viral :type integer)
                               (:name total-tests-people-viral-increase :type integer)
                               (:name total-tests-viral :type integer)
                               (:name total-tests-viral-increase :type integer))
                    :body (vellum:bind-row (date)
                            (setf date (local-time:parse-timestring date)))))

#|
We will want to make a plot of this data, showing us rolling average of the daily ICU occupation, deaths, and new cases, using 7 days window, with the offset from the oldest date seen in the data on the X axis.

All dates are already present in the source file, but in general it is a bad habit to assume it. We are gonna do this right and construct new data frame with each row corresponding to the offset. If there is no data for a selected date, the following method will leave it NULL.
|#

;; First, let's find what is the starting date for all of the data.
(defparameter *start-date*
  (vellum:pipeline (*source-data*)
    (cl-ds.alg:extremum #'local-time:timestamp< :key (vellum:brr date))))

;; We want just those 3 columns.
(defparameter *selected-columns* '(in-icu-currently death-increase positive-increase))

;; and a fresh table


;; local-time calculates timestamp difference in seconds, but we want days offset
(defun seconds->days (seconds)
  (truncate seconds 86400))

(defun offset-from-start (date)
  (seconds->days (local-time:timestamp-difference date *start-date*)))

#|
let's populate the new data frame!
Notice, how we are placing rows into exact spots, as specified by the day offset.
This will handle missing rows, if there are any.
|#
(defparameter *selected-data*
  (vellum:make-table :columns (cons 'day *selected-columns*)))

(vellum:transform *source-data*
                  (vellum:bind-row (date)
                    (let ((offset (offset-from-start date)))
                      (setf (vellum:at *selected-data* offset 'day) offset)
                      (map nil
                           (lambda (column &aux (value (vellum:rr column)))
                             (unless (eq :null value)
                               (setf (vellum:at *selected-data* offset column) value)))
                           *selected-columns*))))

#|
We will simply remove the missing data from the 7 days period.
|#
(defun calculate-average (input-list)
  (let ((cleaned (remove-if (alexandria:curry #'eq :null) input-list)))
    (if (endp cleaned)
        0.0
        (coerce (alexandria:mean cleaned)
                'single-float))))

#|
Lets's calculate those moving averages.
|#
(defun average-data-frame (column)
  (vellum:pipeline (*selected-data*)
    (cl-ds.alg:on-each (alexandria:curry #'vellum:rr column)) ; extract the desired value
    (cl-ds.alg:sliding-window 7) ; 7 days or 1 week
    (vellum:to-table :columns (list column) ; and convert to a single column table
                     :key (alexandria:compose #'list #'calculate-average)))) ; forms an interpretable row

(defparameter *averages*
  (mapcar #'average-data-frame *selected-columns*))

#|
We can simply use hstack function to attach freshly calculated columns to form data frame for our plot.
We will be missing the few first days because not enough data there to calculate average in the first week.
Therefore, drop row.
|#

(defparameter *plot-data*
  (vellum:hstack* (vellum:transform (vellum:select *selected-data* :columns '(day))
                                    (vellum:bind-row (day)
                                      (when (< day 6)
                                        (vellum:drop-row))))
                  *averages*))

#|
Well, let's just plot this, finally!
|#
(vellum:visualize :plotly
                  (vellum-plot:stack *plot-data*
                                     (vellum-plot:aesthetics
                                      :x (vellum-plot:axis
                                          :label "Day from the start")
                                      :y (vellum-plot:axis
                                          :label "Value"))
                                     (vellum-plot:line
                                      :mapping (vellum-plot:mapping :x 'day :y 'in-icu-currently))
                                     (vellum-plot:line
                                      :mapping (vellum-plot:mapping :x 'day :y 'death-increase))
                                     (vellum-plot:line
                                      :mapping (vellum-plot:mapping :x 'day :y 'positive-increase)))
                  "california-plot.html")
