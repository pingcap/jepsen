(ns tidb.g2
  "Tests for some common anomalies in weaker isolation levels"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh letr]]
             [adya :as adya]]
            [jepsen.checker.timeline :as timeline]
            [tidb.sql :refer :all]
            [tidb.basic :as basic]
            [clojure.java.jdbc :as j]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

(defrecord G2Client [node]
  client/Client
  (setup! [this test node]
    (j/with-db-connection [c (conn-spec (first (:nodes test)))]
      (j/execute! c ["create table if not exists a (id int primary key, kk int, value int)"])
      (j/execute! c ["create table if not exists b (id int primary key, kk int, value int)"]))

      (assoc this :node node))

  (invoke! [this test op]
    (with-txn op [c node]
      (let [[k [a-id b-id]] (:value op)]
        (case (:f op)
          :insert
            (letr [order (< (rand) 0.5)
              as (j/query c [(str "select * from " (if order "a" "b") " where kk = ? and value % 3 = 0")
                            k])
              bs (j/query c [(str "select * from " (if order "b" "a") " where kk = ? and value % 3 = 0")
                            k])
              _ (when (or (seq as) (seq bs))
                  ; Ah, the other txn has already committed
                  (return (assoc op :type :fail :error :too-late)))
              table (if a-id "a" "b")
              id    (or a-id b-id)
              r (j/insert! c :table {:id id, :kk k, :value 30})]
            (assoc op :type :ok))

          :read
            (let [as (j/query c ["select * from a where kk = ? and value % 3 = 0" k])
                  bs (j/query c ["select * from b where kk = ? and value % 3 = 0" k])
                  values (->> (concat as bs)
                              (map :id))]
                  (assoc op :type :ok :value (independent/tuple k values)))))))

  (teardown! [this test]))

(defn test
  [opts]
  (basic/basic-test
    (merge
     {:name "g2"
      :client {:client (G2Client. nil)
               :during (adya/g2-gen)}
      :checker (checker/compose {:timeline (timeline/html)
                                 :perf     (checker/perf)
                                 :g2       (adya/g2-checker)})}
     opts)))
