(ns jepsen.system.sirius
    (:require [clojure.tools.logging :refer [debug info warn]]
        [clojure.java.io       :as io]
        [clojure.string        :as str]
        [jepsen.util           :as util :refer [meh timeout]]
        [jepsen.codec          :as codec]
        [jepsen.core           :as core]
        [jepsen.control        :as c]
        [jepsen.control.net    :as net]
        [jepsen.control.util   :as cu]
        [jepsen.client         :as client]
        [jepsen.db             :as db]
        [jepsen.generator      :as gen]
        [knossos.core          :as knossos]
        [clj-http.client       :as webclient]))

(defn setup-sirius!
    [ test node ]
    "Sets up Sirius"
    (info node "setting up Sirius")
    ( c/su
        ; Install
         ( let [ file "Comcast-sirius-reference-app.tar.gz" ]
             ( c/cd "/tmp"
                ( when-not ( cu/file? file )
                    ( info node "Fetching sirius reference app" )
                    ( c/exec :wget "-O" file "https://github.com/Comcast/sirius-reference-app/tarball/master" )
                )

                ( info node "installing Sirius reference app" )
                ( c/exec :tar "-xvf" file )
                ( c/exec :mv "Comcast-sirius-reference-app-0a94721" "sirius-reference-app")
                ( c/cd "sirius-reference-app/java"
                    (info node "running maven")
                    ( c/exec :mvn "clean" "package")
                    (info node "building config")
                    (c/exec :mkdir "example/five-node-remote")
                    ; Config
                    (c/exec :cp "example/single-node-local/config.properties" "example/five-node-remote/config.properties")
                    (c/exec :sed :-i "'s/single-node-local/five-node-remote/g'" "example/five-node-remote/config.properties")
                    (c/exec :sed :-i (str "'s/localhost/" (case (name node)
                                                            "n1" "192.168.122.11"
                                                            "n2" "192.168.122.12"
                                                            "n3" "192.168.122.13"
                                                            "n4" "192.168.122.14"
                                                            "n5" "192.168.122.15") "/g'") "example/five-node-remote/config.properties")
                    (c/exec :echo "akka.tcp://sirius-2552@192.168.122.11:2552/user/sirius" :>  "example/five-node-remote/cluster.config")
                    (c/exec :echo "akka.tcp://sirius-2552@192.168.122.12:2552/user/sirius" :>> "example/five-node-remote/cluster.config")
                    (c/exec :echo "akka.tcp://sirius-2552@192.168.122.13:2552/user/sirius" :>> "example/five-node-remote/cluster.config")
                    (c/exec :echo "akka.tcp://sirius-2552@192.168.122.14:2552/user/sirius" :>> "example/five-node-remote/cluster.config")
                    (c/exec :echo "akka.tcp://sirius-2552@192.168.122.15:2552/user/sirius" :>> "example/five-node-remote/cluster.config")
                    ; Start
                    (info node "starting Sirius")
                    (c/exec "./start_daemon.sh" "example/five-node-remote/config.properties")
                    (Thread/sleep 3000)
                    (info node "setup complete")
                )
             )
         )
    )
)

(defn teardown-sirius!
  [client node]
  ( c/su
  ( c/cd "/tmp"
    ( c/cd "sirius-reference-app/java"
      ( meh ( c/exec "./stop_daemon.sh" ) ))
    ( c/exec :rm :-rf "/tmp/sirius-reference-app")
    ;( c/exec :rm "Comcast-sirius-reference-app.tar.gz")
  )
  ( info node "Sirius torn down" )
  )
)

(def db
  (reify db/DB
    (setup! [_ test node]
      (setup-sirius! test node)
    )

    (teardown! [_ test node]
      (teardown-sirius! test node)
    )
   )
)

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defrecord CreateSetClient [url]
client/Client
  ( setup! [ _ test node ]
    ( let [ url ( str "http://" ( name node ) ":8000" ) ]
      ( info node "node url: " url )
      ( CreateSetClient. url )
    )
  )

  ( teardown! [ _ test ] )

  ( invoke! [ this test op ]
    ( case ( :f op )
      :add  (timeout 6000 (assoc op :type :info :value :timed-out)
            (let [r (webclient/put (str url "/storage/jepsen/" (:value op)) {:body (str (:value op))
                                                                             :throw-exceptions false})]
            (case (:status r)
                200 (assoc op :type :ok)
                (assoc op :type :info :value (:body r)))))
      :read ( timeout 6000 ( assoc op :type :info :value :timed-out )
              (let [r (webclient/get (str url "/keys") {:throw-exceptions false})]
              (case (:status r)
                200 ( assoc op :type :ok :value (set (map parse-int (str/split-lines (str/replace (:body r) #"jepsen/" "")))))
                404 ( assoc op :type :info :value :not-found)
                (assoc op :type :info :value (:body r)))))
    )
  )
)

(defn create-set-client
"A set implemented by creating independent documents"
[]
(CreateSetClient. nil)
)
