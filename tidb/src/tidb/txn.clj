(ns tidb.txn
  "Client for transactional workloads."
  (:require [clojure.string :as str]
            [jepsen [client :as client]]
            [tidb.sql :as c :refer :all]
            [tidb.util :as util]))

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test table-count [f k v]]
  (let [table (table-for table-count k)]
    [f k (case f
           :r (-> conn
                  (c/query [(str "select val from " table " where "
                                 (if (or (:use-index test)
                                         (:predicate-read test))
                                   "sk"
                                   "id")
                                 " = ? "
                                 (:read-lock test))
                            k])
                  first
                  :val)

           :w (do (c/execute! conn [(str "insert into " table
                                         " (id, sk, val) values (?, ?, ?)"
                                         " on duplicate key update val = ?")
                                    k k v v])
                  v)

           :append
           (let [r (c/execute!
                     conn
                     [(str "insert into " table
                           " (id, sk, val) values (?, ?, ?)"
                           " on duplicate key update val = CONCAT(val, ',', ?)")
                      k k (str v) (str v)])]
             v))]))

(defn txn-type [test table-count txn]
  (let [ops  (set (map first txn))
        tbls (set (map #(table-for table-count (second %)) txn))
        single-stmt? (and (:single-stmt-write test)
                          (= 1 (count ops))
                          (= 1 (count tbls)))]
    (cond
      (and single-stmt? (= :w (first ops))) :single-stmt-write
      (and single-stmt? (= :append (first ops))) :single-stmt-append
      (= 1 (count txn)) :query
      :else :txn)))

(defn single-stmt-write! [conn table-count op]
  (let [txn   (:value op)
        table (table-for table-count (second (first txn)))
        query (str "insert into " table " (id, sk, val) values "
                   (str/join ", " (repeat (count txn) "(?, ?, ?)"))
                   " on duplicate key update val = values(val)")
        args  (mapcat (fn [[_ k v]] [k k v]) txn)]
    (c/execute! conn (into [query] args) {:transaction? false})
    (attach-txn-info conn (assoc op :type :ok))))

(defn single-stmt-append! [conn table-count op]
  (let [txn   (:value op)
        table (table-for table-count (second (first txn)))
        query (str "insert into " table " (id, sk, val) values "
                   (str/join ", " (repeat (count txn) "(?, ?, ?)"))
                   " on duplicate key update val = CONCAT(val, ',', values(val))")
        args  (mapcat (fn [[_ k v]] [k k (str v)]) txn)]
    (c/execute! conn (into [query] args) {:transaction? false})
    (attach-txn-info conn (assoc op :type :ok))))

(defrecord Client [conn val-type table-count]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (dotimes [i table-count]
      (c/with-conn-failure-retry conn
        (c/execute! conn [(str "create table if not exists " (table-name i)
                               " (id  int not null primary key,
                               sk  int not null,
                               val " val-type ")")])
        (when (:use-index test)
          (c/create-index! conn [(str "create index " (table-name i) "_sk_val on " (table-name i)
                                      " (sk, val" (when (= val-type "text") "(256)") ")")]))
        (when (:table-cache test)
          (c/execute! conn [(str "alter table " (table-name i) " cache")])))))

  (invoke! [this test op]
    (let [txn (:value op)]
      (condp = (txn-type test table-count txn)

        :single-stmt-write
        (c/with-error-handling op (single-stmt-write! conn table-count op))

        :single-stmt-append
        (c/with-error-handling op (single-stmt-append! conn table-count op))

        :query
        (c/with-error-handling op
          (let [attach-info (if (= :r (-> txn first first)) c/attach-query-info c/attach-txn-info)
                res (mapv (partial mop! conn test table-count) txn)]
            (attach-info conn (assoc op :type :ok, :value res))))

        ;; else
        (c/with-txn op [c conn {:isolation (util/isolation-level test)
                                :before-hook (partial c/rand-init-txn! test conn)}]
          (assoc op :type :ok, :value
                 (mapv (partial mop! c test table-count) txn))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn client
  "Constructs a transactional client. Opts are:

    :val-type     An SQL type string, like \"int\", for the :val field schema.
    :table-count  How many tables to stripe records over."
  [opts]
  (Client. nil
           (:val-type opts "int")
           (:table-count opts 7)))
