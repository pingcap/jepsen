(ns tidb.util)

(defn isolation-level [test] (get test :isolation :repeatable-read))
