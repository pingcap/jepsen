(ns tidb.client
  "For talking to cockroachdb over the network"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen [util :as util :refer [meh]]
                    [reconnect :as rc]
            ]
  )
)

(def max-timeout "Longest timeout, in ms" 30000)
(def timeout-delay "Default timeout for operations in ms" 10000)
(def isolation-level "Default isolation level for txns" :serializable)

(defn db-conn-spec
  "jdbc connection spec for a node."
  [node]
  {:classname   "org.mariadb.jdbc.Driver"
   :subprotocol "mariadb"
   :subname     (str "//" (name node) ":4000/test")
   :user        "root"
   :password    ""
  }
)

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn with-idempotent
  "Takes a predicate on operation functions, and an op, presumably resulting
  from a client call. If (idempotent? (:f op)) is truthy, remaps :info types to
  :fail."
  [idempotent? op]
  (if (and (idempotent? (:f op)) (= :info (:type op)))
    (assoc op :type :fail)
    op))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction."
  [[c conn] & body]
  `(j/with-db-transaction [~c ~conn {:isolation isolation-level}]
     ~@body))

(defn exception->op
  "Takes an exception and maps it to a partial op, like {:type :info, :error
  ...}. nil if unrecognized."
  [e]
  (when-let [m (.getMessage e)]
    (condp instance? e
      java.sql.SQLTransactionRollbackException
      {:type :fail, :error [:rollback m]}

      java.sql.BatchUpdateException
      (if (re-find #"getNextExc" m)
        ; Wrap underlying exception error with [:batch ...]
        (when-let [op (exception->op (.getNextException e))]
          (update op :error (partial vector :batch)))
        {:type :info, :error [:batch-update m]})

      java.sql.SQLException
      (condp re-find (.getMessage e)
        #"Connection .+? refused"
        {:type :fail, :error :connection-refused}

        #"context deadline exceeded"
        {:type :fail, :error :context-deadline-exceeded}

        #"rejecting command with timestamp in the future"
        {:type :fail, :error :reject-command-future-timestamp}

        #"encountered previous write with future timestamp"
        {:type :fail, :error :previous-write-future-timestamp}

        #"restart transaction"
        {:type :fail, :error [:restart-transaction m]}

        {:type :info, :error [:sql-exception m]})

      clojure.lang.ExceptionInfo
      (condp = (:type (ex-data e))
        :conn-not-ready {:type :fail, :error :conn-not-ready}
        nil)

      (condp re-find m
        #"^timeout$"
        {:type :info, :error :timeout}

        nil))))

(defmacro with-exception->op
  "Takes an operation and a body. Evaluates body, catches exceptions, and maps
  them to ops with :type :info and a descriptive :error."
  [op & body]
  `(try ~@body
        (catch Exception e#
          (if-let [ex-op# (exception->op e#)]
            (merge ~op ex-op#)
            (throw e#)))))

(defmacro with-conn
  "Like jepsen.reconnect/with-conn, but also asserts that the connection has
  not been closed. If it has, throws an ex-info with :type :conn-not-ready.
  Delays by 1 second to allow time for the DB to recover."
  [[c client] & body]
  `(rc/with-conn [~c ~client]
     (when (.isClosed (j/db-find-connection ~c))
       (Thread/sleep 1000)
       (throw (ex-info "Connection not yet ready."
                       {:type :conn-not-ready})))
     ~@body))

(defmacro with-timeout
  "Like util/timeout, but throws (RuntimeException. \"timeout\") for timeouts.
  Throwing means that when we time out inside a with-conn, the connection state
  gets reset, so we don't accidentally hand off the connection to a later
  invocation with some incomplete transaction."
  [& body]
  `(util/timeout timeout-delay
                 (throw (RuntimeException. "timeout"))
                 ~@body))

(defn client
  "Constructs a network client for a node, and opens it"
  [node]
  (rc/open!
    (rc/wrapper
      {:name [str node]
       :open (fn open []
               (util/timeout max-timeout
                             (throw (RuntimeException.
                                      (str "Connection to " node " timed out")))
                             (let [spec (db-conn-spec node)
                                   conn (j/get-connection spec)
                                   spec' (j/add-connection spec conn)]
                               (assert spec')
                               spec')))
       :close close-conn
       :log? true})))

(defn query
  "Like jdbc query, but includes a default timeout in ms."
  ([conn expr]
   (query conn expr {}))
  ([conn [sql & params] opts]
   (let [s (j/prepare-statement (j/db-find-connection conn)
                                sql
                                {:timeout (/ timeout-delay 1000)})]
     (try
       (j/query conn (into [s] params) opts)
       (finally
         (.close s))))))

(defn insert!
  "Like jdbc insert!, but includes a default timeout."
  [conn table values]
  (j/insert! conn table values {:timeout timeout-delay}))

(defn insert-with-rowid!
  "Like insert!, but includes the auto-generated :rowid."
  [conn table record]
  (let [keys (->> record
                  keys
                  (map name)
                  (str/join ", "))
        placeholder (str/join ", " (repeat (count record) "?"))]
    (merge record
           (first (query conn
                         (into [(str "insert into " table " (" keys
                                     ") values (" placeholder
                                     ") returning rowid;")]
                               (vals record)))))))

(defn update!
  "Like jdbc update!, but includes a default timeout."
  [conn table values where]
  (j/update! conn table values where {:timeout timeout-delay}))
