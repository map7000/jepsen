(ns jepsen.ignite.counter
    "Incrementing a counter"
    (:require [clojure [pprint :refer [pprint]]
               [string :as str]]
      [clojure.java.io :as io]
      [clojure.tools.logging :refer [debug info warn]]
      [jepsen
       [ignite :as ignite]
       [checker :as checker]
       [client :as client]
       [nemesis :as nemesis]
       [generator :as gen]
       [independent :as independent]]
      [jepsen.checker.timeline :as timeline]
      [knossos.model :as model])
    (:import (org.apache.ignite Ignition IgniteCache)
      (clojure.lang ExceptionInfo)))

(defrecord Client [conn seq config]
           client/Client
           (open! [this test node]
                  (let [config (ignite/configure-client (:nodes test))
                        conn (Ignition/start (.getCanonicalPath config))]
                       (assoc this :conn conn :config config)))

           (setup! [this test]
                   (let [seq (.atomicSequence conn "JepsenSequence" 0 true)]
                        (assoc this :seq seq)))

           (invoke! [this test op]
                    (try
                      (case (:f op)
                            :read (let [value (.get seq)]
                                       (assoc op :type :ok :value value))

                            :add (do (.getAndIncrement seq)
                                     (assoc op :type :ok)))))

           (teardown! [this test]
                      (.delete config))

           (close! [_ test]
                   (.close conn)))

(defn r [_ _] {:type :invoke, :f :read})
(defn add [_ _] {:type :invoke, :f :add})

(defn workload
      []
      {:client    (Client. nil nil nil)
       :generator (->> (repeat 100 add)
                       (cons r)
                       gen/mix
                       (gen/delay 1/100))
       :checker   (checker/counter)})

(defn test
      [opts]
      (ignite/basic-test
        (merge
          {:name      "counter-test"
           :client    (Client. nil nil nil)
           :checker   (checker/counter)
           :generator (ignite/generator [r add] (:time-limit opts))}
          opts)))