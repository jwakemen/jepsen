(ns jepsen.tests
  (:use jepsen.core
        clojure.test
        clojure.pprint
        clojure.tools.logging)
  (:require [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.control :as control]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.model :as model]
            [jepsen.checker :as checker]))

(def noop-test
"Boring test stub"
  {:nodes     [:n1 :n2 :n3 :n4 :n5]
   :os        os/noop
   :db        db/noop
   :client    client/noop
   :nemesis   client/noop
   :generator gen/void
   :model     model/noop
   :checker   checker/linearizable})

(defn atom-db
 "Wraps an atom as a database."
 [state]
 (reify db/DB
     (setup!    [db test node] (reset! state 0))
     (teardown! [db test node] (reset! state :done))))

(defn atom-client
  "A CAS client which uses an atom for state."
  [state]
  (reify client/Client
    (setup!    [this test node] this)
    (teardown! [this test])
    (invoke!   [this test op]
      (case (:f op)
        :write (do (reset! state   (:value op))
                   (assoc op :type :ok))

        :cas   (let [[cur new] (:value op)]
                  (try
                     (swap! state (fn [v]
                                    (if (= v cur)
                                      new
                                      (throw (RuntimeException. "CAS failed")))))
                 (assoc op :type :ok)
                 (catch RuntimeException e
                   (assoc op :type :fail))))

        :read  (assoc op :type :ok
                         :value @state)))))