(ns jepsen.ignite.nemesis
  "Apache Ignite nenesis"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [jepsen [ignite :as ignite]
                    [nemesis :as nemesis]]))

(def node-start-stopper
  "Kills random node"
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (ignite/stop! node test))
    (fn stop [test node] (ignite/start! node test))))

(def partition-random-halves
  (nemesis/partition-random-halves))

(def partition-halves
  (nemesis/partition-halves))

(def majorities-ring
  (nemesis/majorities-ring))

(def partition-random-node
  (nemesis/partition-random-node))

(def partition-majorities-ring
  (nemesis/partition-majorities-ring))

(def bridge
  (nemesis/bridge))

(def clock-scrambler
  (nemesis/clock-scrambler 10))


;(defn slowing
;      "Wraps a nemesis. Before underlying nemesis starts, slows the network by dt
;      s. When underlying nemesis resolves, restores network speeds."
;      [nem dt]
;      (reify nemesis/Nemesis
;             (setup! [this test]
;                     (net/fast! (:net test) test)
;                     (nemesis/setup! nem test)
;                     this)
;
;             (invoke! [this test op]
;                      (case (:f op)
;                            :start (do (net/slow! (:net test) test {:mean (* dt 1000) :variance 1})
;                                       (nemesis/invoke! nem test op))
;
;                            :stop (try (nemesis/invoke! nem test op)
;                                       (finally
;                                         (net/fast! (:net test) test)))
;
;                            (nemesis/invoke! nem test op)))
;
;             (teardown! [this test]
;                        (net/fast! (:net test) test)
;                        (nemesis/teardown! nem test))))
