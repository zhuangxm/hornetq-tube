(ns hornetq-tube.core
  (require [hornetq-clj.core-client :as msg])
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

(defprotocol SwitchBoard
  (send-msg [this msg] [this msg address] "send msg to address")
  (receive-msg [this f address] [this f address uniq-queue-name]
    "receive message from address")
  (close-switch [this] "close the switchboard"))

(defn create-switch
  [{:keys [host port send-address receive-queue]
    :or {host "localhost" port 5445} :as options}]
  (let [^ClientSessionFactory factory (netty-session-factory options)
        ^ClientSession session (msg/session factory nil nil nil)
        ^ClientProducer producer (msg/create-producer session)
        ^LinkedBlockingQueue q (LinkedBlockingQueue.)
        fn-send (fn [] (let [m (.take q)
                            c-m (msg/create-message session false)]
                        (msg/send-message producer
                                          (msg/write-message c-m
                                                             (:message m))
                                          (:address m)) ))]
    (do (.start session)
        (.start (Thread.  fn-send) )
        (reify SwitchBoard
          (send-msg
            [this message]
            (send-msg this message send-address))
          (send-msg [this message address]
            (let [^ClientMessage m (msg/create-message session false)]
              (.put q {:message message :address address})
              (msg/send-message producer
                                (msg/write-message m message) address)))
          (receive-msg
            [this f address]
            (receive-msg this f address (uuid)))
          (receive-msg
            [this f address uniq-queue-name]
            (let [queue (msg/ensure-queue session uniq-queue-name
                                          {:address address})
                  ^ClientConsumer consumer (msg/create-consumer session uniq-queue-name nil)]
              (.setMessageHandler consumer (msg/message-handler
                                            #(f (msg/read-message %))))))
          (close-switch [this] (.close session))))))


