(ns jepsen.ignite.sqlindex
    "Simulates transfers between bank accounts"
    (:refer-clojure :exclude [test])
    (:require [clojure.tools.logging :refer :all]
      [jepsen [ignite :as ignite]
       [checker :as checker]
       [client :as client]
       [nemesis :as nemesis]
       [generator :as gen]]
      [clojure.core.reducers :as r]
      [jepsen.checker.timeline :as timeline]
      [knossos.model :as model]
      [knossos.op :as op])
    (:import (org.apache.ignite Ignition)
      (org.apache.ignite.transactions TransactionConcurrency TransactionIsolation)
      (org.apache.ignite.transactions TransactionTimeoutException)
      (org.apache.ignite.cache CacheMode CacheAtomicityMode CacheWriteSynchronizationMode)))

(def n 10)

(def cache-name "TRANSACTIONS")

(defn client-config [servers]
      (let [config (ClientConfiguration.)]
           (.setAddresses config servers)
           config))

(defrecord IndexClient
           [conn]
           client/Client
           (open! [this test node]
                  (let [conn ignite (Ignition/startClient (client-config (into-array (:nodes test))))]
                       (assoc this :conn conn)))
           (setup! [this test]
                   (.getAll (.query conn
                                    (SqlFieldsQuery. "CREATE TABLE IF NOT EXISTS PERSON (id int PRIMARY KEY, name varchar)")))
                   (.getAll (.query conn
                                    (SqlFieldsQuery. "CREATE INDEX IF NOT EXISTS NAME_IDX ON PERSON (name) INLINE_SIZE 48")))
                   )
           (invoke! [_ test op]
                    (try
                      (case (:f op)
                            :get-transactions (let [value (.next (.iterator
                                                                   (.next (.iterator
                                                                            (.getAll (.query ignite
                                                                                             (SqlFieldsQuery. (str "SELECT count(*) from PERSON WHERE id="(:value op)))))))))]
                                                   (assoc op :type :ok, :value value))
                            :transaction (try
                                          (.getAll (.query ignite
                                                           (SqlFieldsQuery. "INSERT INTO PERSON(id, name) VALUES(1, 3)")))
                                          (assoc op :type :ok)
                                          (catch Exception e (info (.getMessage e)) (assoc op :type :fail, :error (.printStackTrace e)))

                                          ))))
           (teardown! [this test])
           (close! [this test]
                   (.getAll (.query ignite
                                    (SqlFieldsQuery. "DROP TABLE IF EXISTS PERSON")))
                   (.close conn)))

(defn get-transactions
      "Count transactions on customer"
      [_ _]
      {:type :invoke
       :f :get-transactions
       :value (rand-int n)})

(defn transaction
      "Make a transaction"
      [_ _]
      {:type  :invoke
       :f     :transaction
       :value (rand-int n)})

(defn test
      [opts]
      (ignite/basic-test
        (merge
          {:name      "index-test"
           :client    (IndexClient. nil)
           :checker   (checker/compose
                        {:perf     (checker/perf)
                         :timeline (timeline/html)
                         :details  (jepsen.checker/unbridled-optimism)})
           :generator (ignite/generator [transaction get-transactions] (:time-limit opts))}
          opts)))
