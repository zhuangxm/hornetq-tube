(ns hornetq-tube.core
  (require [hornetq-clj.core-client :as msg]
           [hornetq-clj.server :as server]
           [clojure.tools.logging :as log])
  (import
   [org.hornetq.api.core
    HornetQException SimpleString TransportConfiguration]
   [org.hornetq.api.core.client
    ClientConsumer ClientMessage ClientProducer ClientSession
    ClientSessionFactory HornetQClient MessageHandler ServerLocator]
   [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]
   [java.util.concurrent LinkedBlockingQueue]))

(defn uuid
  "generate unique ID"
  []
  (str (java.util.UUID/randomUUID)))

(defn netty-session-factory
  "Create a session factory for the given host and port"
  [{:keys [host port] :or {host "localhost" port 5445} :as options}]
  (let [^TransportConfiguration transport (TransportConfiguration.
                                           (.getName NettyConnectorFactory)
                                           {"host" host "port" port})] 
    (-> (HornetQClient/createServerLocatorWithoutHA
         (into-array TransportConfiguration [transport]))
        (doto (.setReconnectAttempts -1))
        (.createSessionFactory))))

(defprotocol Tube
  (send-msg [this msg] [this msg address] "send msg to address")
  (receive-msg [this f address] [this f address uniq-queue-name]
    "receive message from address")
  (close-tube [this] "close the tube"))

(defn- send-message
  [^ClientSession session ^ClientProducer producer m]
  (let [^ClientMessage c-m (msg/create-message session false)]
    (msg/send-message producer
                      (msg/write-message c-m (:message m))
                      (:address m))))

(defn create-tube
  "create a tube using to communicate with hornetq server"
  [& [{:keys [host port send-address receive-queue]
        :or {host "localhost" port 5445} :as options}]]
  (let [^ClientSessionFactory factory (netty-session-factory options)
        ^ClientSession session (msg/session factory nil nil nil)
        ^ClientProducer producer (msg/create-producer session)
        ^LinkedBlockingQueue q (LinkedBlockingQueue.)
        fn-send (fn []
                  (while true
                    (try 
                      (send-message session producer (.take q))
                      (catch Exception e
                        (log/error "send messsage error", e)))))]
    (do (.start session)
        (.start (Thread.  fn-send) )
        (reify Tube
          (send-msg
            [this message]
            (send-msg this message send-address))
          (send-msg [this message address]
            (.put q {:message message :address address}))
          (receive-msg
            [this f address]
            (receive-msg this f address (uuid)))
          (receive-msg
            [this f address uniq-queue-name]
            (let [queue (msg/ensure-temporary-queue session uniq-queue-name
                                                    :address address)
                  ^ClientConsumer consumer (msg/create-consumer session uniq-queue-name nil)]
              (.setMessageHandler consumer (msg/message-handler
                                            #(f (msg/read-message %))))))
          (close-tube [this]
            (do (.close session)
                (.close factory)))))))

(defn create-tube-server
  ([] (create-tube-server "127.0.0.1" 5445))
  ([host] (create-tube-server host 5445))
  ([host port]
     (let [server  (server/make-server
                    {:acceptors [{:factory-class-name
                                  server/netty-acceptor-factory
                                  :host host :port port}]
                     :connectors [{:name "netty-connector"
                                   :factory-class-name
                                   server/netty-connector-factory}]})]
       (do (.start server)
           server))))

(defn stop-tube-server
  [server]
  (.stop server))


