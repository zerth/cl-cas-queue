;;;; a lock-free queue implementation for LispWorks, CCL, and SBCL.

(defpackage :net.mwatters.cas-queue
  (:nicknames :cas-queue)
  (:use :common-lisp))

(in-package :cas-queue)


(defconstant +cas-sleep+ (/ 1 internal-time-units-per-second)
  "The initial duration in seconds for which a thread should sleep
while in a CAS retry loop.")

(defconstant +max-cas-sleep+ 0.1
  "The maximum duration in seconds for which a thread in a CAS retry
loop should sleep.")


(defmacro cas (place old new)
  "Atomically attempt to set the new value of PLACE to be NEW if it
was EQ to OLD, returning non-nil if successful."
  #+lispworks `(sys:compare-and-swap ,place ,old ,new)
  #+ccl `(ccl::conditional-store ,place ,old ,new)
  #+sbcl (let ((ov (gensym "OLD")))
           `(let ((,ov ,old))
              (eq ,ov (sb-ext:compare-and-swap ,place ,ov ,new))))
  #-(or lispworks ccl sbcl) (error "fixme; implement CAS"))


(defmacro atomic-incf (place)
  "Atomically increment the value of PLACE.  For CCL, SBCL, and LW, it
should be an accessor form for a struct slot holding an integer."
  #+lispworks `(sys:atomic-incf ,place)
  #+ccl `(ccl::atomic-incf ,place)
  #+sbcl `(sb-ext:atomic-incf ,place)
  #-(or lispworks ccl sbcl) (error "fixme; implement ATOMIC-INCF"))


(defmacro with-cas-retry (&body forms)
  "Execute FORMS with RETRY lexically bound to a function which sleeps
for the current CAS sleep interval before retrying FORMS.  The sleep
interval starts at +CAS-SLEEP+ and exponentially increases up to
+MAX-CAS-SLEEP+."
  (let ((b (gensym "BLOCK"))
        (r (gensym "RETRY"))
        (f (gensym "F"))
        (w (gensym "WAITTIME")))
    `(let ((,w +cas-sleep+))
       (block ,b
         (tagbody
          ,r
          (flet ((retry ()
                   (sleep ,w)
                   (setq ,w (min +max-cas-sleep+
                                 (* 1.5 ,w)))
                   (go ,r)))
            (flet ((,f () ,@forms))
              (return-from ,b (,f)))))))))


(defconstant +empty+ (if (boundp '+empty+)
                         (symbol-value '+empty+)
                       (gensym "EMPTY"))
  "sentinel for the value of an empty or popped queue cell.")


;; note: some implementations only support compare-and-swap on struct
;; slot places, so we use a struct for queue cells:

(defstruct (queue-cell
            (:constructor make-queue-cell (&optional value)))
  (value +empty+) ; a value or +empty+
  (next nil :type t)) ; a queue cell or nil


;; note: the queue counts can overflow/wraparound on 32-bit sbcl
(defstruct queue
  head
  tail
  (dequeued-count (make-array 1 :element-type #+sbcl 'sb-ext:word
                                              #-sbcl 'integer
                                :initial-element 0)
                  :type #+sbcl (simple-array sb-ext:word (*))
                        #-sbcl (vector integer))
  (enqueued-count (make-array 1 :element-type #+sbcl 'sb-ext:word
                                              #-sbcl 'integer
                                :initial-element 0)
                  :type #+sbcl (simple-array sb-ext:word (*))
                        #-sbcl (vector integer)))


(defmacro queue-atomic-incf (q accessor)
  `(atomic-incf (#+sbcl aref
                 #-sbcl svref (,accessor ,q) 0)))


(defun queue-count (q)
  "Return the approximate number of items in the queue."
  (max 0 (- (#+sbcl aref
             #-sbcl svref (queue-enqueued-count q) 0)
            (#+sbcl aref
             #-sbcl svref (queue-dequeued-count q) 0))))


(defun queue-push (queue item)
  "Add ITEM to the tail of QUEUE, returning as values ITEM and QUEUE."
  (symbol-macrolet
      ((head (queue-head queue))
       (tail (queue-tail queue)))
    (let ((new (make-queue-cell item)))
      (with-cas-retry
        (values (let ((cur tail))
                  ;; if we have no head, update it:
                  (cas head nil cur)

                  (cond
                   ;; no current tail, try to add:
                   ((null cur)
                    (unless (cas tail nil new)
                      (retry))
                    ;; if we had no tail, we had no head:
                    (setf head new))
                   ;; try to update old tail next pointer to point to
                   ;; the new item:
                   (t
                    (unless (cas (queue-cell-next cur) nil new)
                      (retry))
                    ;; if there was already a tail item in the queue,
                    ;; its next pointer now points to the new item.
                    ;; at this point, other threads trying to push
                    ;; items will retry until we update the tail
                    ;; pointer:
                    (setf tail new)))

                  ;; the tail has been updated, and the old tail cell
                  ;; now points to the new tail if the old tail
                  ;; existed.  if the old tail didn't exist, one
                  ;; thread will have succeeded in setting a new tail.

                  (queue-atomic-incf queue queue-enqueued-count)

                  item)
                queue)))))


(defun queue-empty-p (queue)
  "Return non-nil when QUEUE is \(probably\) empty."
  (with-cas-retry
    (let ((head (queue-head queue)))
      (or (null head)
          ;; when a queue cell is popped, its value is set to the
          ;; sentinel value +empty+.  if we see that value, our view
          ;; of the queue is inconsistent and we must retry:
          (when (eq +empty+ (queue-cell-value head))
            (retry))))))


(defun queue-peek (queue)
  "Return a value \(if any\) which was recently stored in the head of
QUEUE."
  (with-cas-retry
    (let ((head (queue-head queue)))
      (when head
        (let ((v (queue-cell-value head)))
          (when (eq +empty+ v)
            (retry))
          v)))))


(defun queue-dequeue (queue)
  "Dequeue the next item from the head of QUEUE."
  (symbol-macrolet
      ((head (queue-head queue)))
    (with-cas-retry
      (let ((cur head))
        (cond
         ((null cur)
          (values nil t))
         (t
          (let ((value (queue-cell-value cur))
                (next (queue-cell-next cur)))
            ;; if this happens, the head caught up with the tail at
            ;; some point in the past:
            (when (eq +empty+ value)
              (cas head cur next)
              (retry))

            ;; try to update head to point to next.  if another thread
            ;; already did, retry:
            (unless (cas head cur next)
              (retry))
            ;; the queue's head pointer has now been updated to point
            ;; to NEXT, which may be nil.  if nil, at this point
            ;; another thread may update the head slot.

            ;; mark old head cell as an empty cell:
            #+with-assertions (assert (not (eq +empty+ (queue-cell-value cur))))
            (setf (queue-cell-value cur) +empty+)

            (queue-atomic-incf queue queue-dequeued-count)

            (values value nil))))))))


(defun queue-pop (queue &key wait-p)
  "Pop the next item from QUEUE, returning as values the item which
was popped and whether the queue was empty.  If WAIT-P is a number,
busy-wait up to that many seconds to pop an item.  If WAIT-P is
otherwise non-nil, busy-wait forever."
  (cond
   (wait-p
    (let ((end-time (when (numberp wait-p)
                      (+ (get-internal-real-time)
                         (* wait-p internal-time-units-per-second)))))
      (block nil
        (with-cas-retry
          (multiple-value-bind (item empty-p)
              (queue-dequeue queue)
            (unless empty-p
              #+with-assertions (assert (not (eq +empty+ item)))
              (return (values item empty-p))))
          (when (and end-time
                     (>= (get-internal-real-time) end-time))
            (return (values nil t)))
          (retry)))))

   (t
    (queue-dequeue queue))))




(deftest:deftest "basic queue tests" ()
  (let ((q (make-queue))
        (items (loop for i below 4096 collect i)))
    (dolist (i items)
      (queue-push q i)
      (assert (eql 0 (queue-peek q)))
      (assert (null (queue-cell-next (queue-tail q)))))
    (dolist (i items)
      (assert (eql i (queue-pop q)))
      (assert (or (not (queue-tail q))
                  (null (queue-cell-next (queue-tail q)))))
      (let ((next (queue-peek q)))
        (assert (or (not next)
                    (eql (1+  i) next)))))))


#+lispworks
(deftest:deftest "single writer/single-reader queue tests" ()
  (let* ((q (make-queue))
         (items (loop for i below 524288 collect i))
         (popped (list))
         (popper (mp:process-run-function
                  "queue popper" ()
                  (lambda ()
                    (loop
                     do (multiple-value-bind (value empty-p)
                            (queue-pop q :wait-p 10)
                          (when empty-p
                            (loop-finish))
                          (push value popped))))))
         (pusher (mp:process-run-function
                  "queue pusher" ()
                  (lambda ()
                    (dolist (i items)
                      (queue-push q i)
                      (sleep (random 0.0001)))))))
    (mp:process-wait "waiting for queue workers"
                     (let ((procs (list popper pusher)))
                       (lambda ()
                         (notany #'mp:process-alive-p procs))))
    (assert (equalp items
                    (sort popped #'<)))))


#+lispworks
(deftest:deftest "single writer/multi-reader queue tests" ()
  (let* ((q (make-queue))
         (items (loop for i below 524288 collect i))
         (pusher (mp:process-run-function
                  "queue pusher" ()
                  (lambda ()
                    (dolist (i items)
                      (queue-push q i)
                      (sleep (random 0.0001)))))))
    (loop
     repeat 8
     for x = (list (list))
     collect x into results
     collect (mp:process-run-function
              "queue popper" ()
              (let ((x x))
                (lambda ()
                  (loop
                   do
                   (multiple-value-bind (value empty-p)
                       (queue-pop q :wait-p 10)
                     (when empty-p
                       (loop-finish))
                     (push value (first x))))))) into procs
     finally (progn
               (mp:process-wait "waiting for queue workers"
                                (let ((procs (cons pusher procs)))
                                  (lambda ()
                                    (notany #'mp:process-alive-p procs))))
               (assert (equalp items
                               (sort (apply #'append (mapcar #'car results)) #'<)))))))


#+lispworks
(deftest:deftest "multi-writer/multi-reader queue tests" ()
  (let ((q (make-queue))
        (items (loop for i below 65536 collect i)))
    (loop
     repeat 8
     for x = (list (list))
     collect x into results
     collect (mp:process-run-function
              "queue popper" ()
              (let ((x x))
                (lambda ()
                  (loop
                   do
                   (multiple-value-bind (value empty-p)
                       (queue-pop q :wait-p 10)
                     (when empty-p
                       (loop-finish))
                     (push value (first x))))))) into procs
     collect (mp:process-run-function
              "queue pusher" ()
              (lambda ()
                (dolist (i items)
                  (queue-push q i)
                  (sleep (random 0.0001))))) into procs
     appending items into expected
     finally (progn
               (mp:process-wait "waiting for queue workers"
                                (lambda ()
                                  (notany #'mp:process-alive-p procs)))
               (assert (equalp (sort expected #'<)
                               (sort (apply #'append (mapcar #'car results)) #'<)))))))
