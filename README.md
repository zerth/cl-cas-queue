cas-queue
=========

Basic lock-free queue implementation for LispWorks, CCL, and SBCL.
Implemented using compare-and-swap.

Usage:

```
(defvar *q* (cas-queue:make-queue))

(defun push-item (x)
  (cas-queue:queue-push *q* x))

(defun pop-item ()
  (cas-queue:queue-pop *q* :wait-p t))
```
