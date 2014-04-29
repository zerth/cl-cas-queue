; -*- mode: lisp -*-

(asdf:defsystem :cas-queue
  :description "Basic lock-free queue implementation for LispWorks, CCL, and SBCL."
  :version "0.1.1"
  :license "MIT"
  :author "Mike Watters <mike@mwatters.net>"
  :depends-on (:deftest)
  :components ((:file "cas-queue")))
