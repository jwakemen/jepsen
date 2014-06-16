(ns jepsen.system.sirius-test
  (:use jepsen.system.sirius
        jepsen.core
        jepsen.core-test
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.util      :as util]
            [jepsen.os.debian :as debian]
            [jepsen.tests     :as tst]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]
            [jepsen.report    :as report]))

(deftest create-test
    (let [test (run!
        ( assoc
            tst/noop-test
            :name "sirius-dummy"
            :os        debian/os
            :db        db
            :client    (create-set-client)
            :model     (model/set)
            :checker   (checker/compose {:html timeline/html
                                         :set  checker/set})
            ;:nemesis   (nemesis/partitioner nemesis/bridge)
            :nemesis   (nemesis/partition-random-halves)
            :generator (gen/phases
                           (->> (range)
                               (map (fn [x] {:type  :invoke
                                             :f     :add
                                             :value x}))
                                gen/seq
                               (gen/stagger 1/10)
                               (gen/delay 1)
                               (gen/nemesis
                                   (gen/seq
                                       (cycle [(gen/sleep 60)
                                               {:type :info :f :start}
                                                   (gen/sleep 300)
                                               {:type :info :f :stop}])))
                                   (gen/time-limit 600))
                               (gen/nemesis
                                   (gen/once {:type :info :f :stop}))
                               (gen/clients
                                   (gen/once {:type :invoke :f :read}))
                         )))]
    (is (:valid? (:results test)))
    (pprint (:results test))))