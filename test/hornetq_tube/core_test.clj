(ns hornetq-tube.core-test
  (:use [midje.sweet]
        [hornetq-tube.core]))

(def ^:dynamic *server* (atom nil))

(defn setup
  []
  (reset! *server* (create-tube-server)))

(defn teardown
  []
  (.stop @*server*))

(def test-messages (range 1000))

(def ^:dynamic *remains-message* (atom test-messages))

(defn consume [m]
  (swap! *remains-message* #(if (= m (first %)) (rest %) %)))

(against-background [(before :contents (setup))
                     (after :contents (teardown))]
  (facts "create a server and send and receive message"
    (let [tube (create-tube)
          _ (receive-msg tube consume "address")
          _ (doseq [m test-messages] (send-msg tube m "address"))
          _ (Thread/sleep 1000)
          _ (close-tube tube)]
      (empty? @*remains-message*) => truthy)))
