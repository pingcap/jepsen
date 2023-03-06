(ns tidb.util
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [jepsen.tests.cycle :refer [DataExplainer directed-graph link]]))

(defn isolation-level [test] (get test :isolation :repeatable-read))

(defn fail-api
  ([node port]
   (fail-api node port ""))
  ([node port failpoint]
   (str "http://" (name node) ":" port "/fail/" failpoint)))

(defn fail-enabled?
  [node port]
  (= 200 (-> (fail-api node port)
             (http/get {:throw-exceptions false})
             (:status))))

(defn fail-enable!
  [node port failpoint value]
  (-> (fail-api node port failpoint)
      (http/put {:body value})))

(defn fail-disable!
  [node port failpoint]
  (-> (fail-api node port failpoint)
      (http/delete)))

(defn fail-list
  [node port]
  (->> (fail-api node port)
       (http/get)
       (:body)
       (str/split-lines)
       (map #(str/split % #"=" 2))
       (filter #(not (empty? (second %))))
       (into {})))

(defn get-start-ts [op] (get-in op [:txn-info :start_ts] 0))
(defn get-commit-ts [op] (get-in op [:txn-info :commit_ts] 0))

(def max-ts 18446744073709551615N)

(defrecord TSOExplainer []
  DataExplainer
  (explain-pair-data
    [_ a b]
    {:type :tso :a a :b b})
  (render-explanation
    [_ {:keys [a b]} a-name b-name]
    (str a-name "'s commit-ts " (get-commit-ts a) " < " b-name "'s start-ts " (get-start-ts b))))

(defn tso-graph [history]
  (loop [h (->> history
                (filter #(let [start-ts (get-start-ts %)]
                           (and (pos? start-ts)
                                (not= max-ts start-ts))))
                (sort-by get-start-ts))
         g (directed-graph)
         ops nil]
    (if-let [op (first h)]
      (let [start-ts (get-start-ts op)
            commit-ts (get-commit-ts op)
            link-to-op (fn [g op'] (link g op' op :tso))
            before-op? (fn [op'] (< (get-commit-ts op') start-ts))
            ops' (filter before-op? ops)
            implied-ts (when (seq ops') (apply max (map get-start-ts ops')))
            after-implied-ts? (fn [op'] (>= (get-commit-ts op') implied-ts))]
        (cond
          (zero? commit-ts)
          (recur (next h)
                 (reduce link-to-op g ops')
                 ops)

          implied-ts
          (recur (next h)
                 (reduce link-to-op g (filter after-implied-ts? ops'))
                 (cons op (filter after-implied-ts? ops)))

          :else
          (recur (next h)
                 g
                 (cons op ops))))
      [g (TSOExplainer.)])))
