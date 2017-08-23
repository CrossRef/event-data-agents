(ns event-data-agents.agents.wikipedia.core
  (:require [event-data-agents.util :as util]
            [event-data-common.evidence-log :as evidence-log]
            [crossref.util.doi :as cr-doi]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.core.async :refer [alts!! timeout thread buffer chan <!! >!!]]
            [clj-http.client :as client]
            [config.core :refer [env]]
            [clj-time.core :as clj-time]
            [throttler.core :refer [throttle-fn]]
            [clj-time.coerce :as clj-time-coerce]
            [clj-time.format :as clj-time-format]
            [robert.bruce :refer [try-try-again]])
  (:import [java.net URLEncoder]
           [java.util UUID]
           [org.apache.commons.codec.digest DigestUtils])
   (:gen-class))

(def source-token "36c35e23-8757-4a9d-aacf-345e9b7eb50d")
(def source-id "wikipedia")
(def agent-name "wikipedia-agent")

(def stream-url "https://stream.wikimedia.org/v2/stream/recentchange")
(def
  action-chunk-size
  "Number of input actions to batch into events. A value of 100 results in message size of about 150 KB"
  100)

(def action-input-buffer 1000000)
(def action-chan (delay (chan action-input-buffer (partition-all action-chunk-size))))

(declare manifest)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(defn parse
  "Parse an input into a Percolator Action. Note that an Action ID is not supplied."
  [data]
  (when (= "edit" (:type data))
    (let [canonical-url (-> data :meta :uri)
          encoded-title (->> data :meta :uri (re-find #"^.*?/wiki/(.*)$") second)
          title (:title data)

          action-type (:type data)
          old-id (-> data :revision :old)
          new-id (-> data :revision :new)

          old-pid (str (:server_url data) "/w/index.php?title=" encoded-title "&oldid=" old-id)
          new-pid (str (:server_url data) "/w/index.php?title=" encoded-title "&oldid=" new-id)

          new-restbase-url (str (:server_url data) "/api/rest_v1/page/html/" encoded-title "/" new-id)

          ; normalize format
          timestamp-str (clj-time-format/unparse date-format (clj-time-coerce/from-string (-> data :meta :dt)))]

      {:url new-pid
       :occurred-at timestamp-str
       :relation-type-id "references"
       :subj {
         :pid new-pid
         :url canonical-url
         :title title
         :api-url new-restbase-url}
       :observations [{:type :content-url
                       :input-url new-restbase-url
                       ; The Wikipedia robots.txt files prohibit access to the API.
                       ; There are separate terms for the RESTBase API, plus we have specific permission.
                       :ignore-robots true
                       :sensitive true}]
     :extra-events [
       {:subj_id old-pid
        :relation_type_id "is_version_of"
        :obj_id canonical-url
        :occurred_at timestamp-str}
       {:subj_id new-pid
        :relation_type_id "is_version_of"
        :obj_id canonical-url
        :occurred_at timestamp-str}
       {:subj_id new-pid
        :relation_type_id "replaces"
        :obj_id old-pid
        :occurred_at timestamp-str}]})))


(defn ingest-into-channel
  [channel]
  "Send parsed events to the chan and block.
   On exception, log and exit (allowing it to be restarted)"
    
  (evidence-log/log! {
    :i "a0026" :s agent-name :c "wikipedia-api" :f "connect"})

  (try
    (let [response (client/get stream-url {:as :stream})]
      (with-open [reader (io/reader (:body response))]
        (doseq [line (line-seq reader)]
          (let [timeout-ch (timeout 1000)
                result-ch (thread (or line :timeout))
                [x chosen-ch] (alts!! [timeout-ch result-ch])]
              ; timeout: x is nil, xs is nil
              ; null from server: x is :nil, xs is rest
              ; data from serer: x is data, xs is rest
              (cond
                (nil? x) nil
                (= :timeout x) nil
                :default
                    (when (.startsWith x "data:")
                      (when-let [parsed (parse (json/read-str (.substring x 5) :key-fn keyword))]
                        (>!! channel parsed))))))))
                    
    (catch Exception ex
      ; Could be SocketException, UnknownHostException or something worse!
      ; log and exit.
      (log/error "Error subscribing to stream" (.getMessage ex))

      (evidence-log/log! {
        :i "a0027" :s agent-name :c "wikipedia-api" :f "disconnect"})

      (.printStackTrace ex))))

(defn main-ingest-stream
  "Subscribe to the WC Stream, put data in the input-stream-chan."
  []


  (log/info "Start ingest stream!")
  (loop []
    (evidence-log/log! {
      :i "a0028" :s agent-name :c "ingest" :f "start"})

    (try
      (log/info "Starting...")
      (ingest-into-channel @action-chan)
    (catch Exception ex (do
      (log/error "Unhandled exception" (.getMessage ex))
      (evidence-log/log! {
        :i "a003e" :s agent-name :c "ingest" :f "error"})))
    (finally
      (log/error "Stopped!")
      (Thread/sleep 1000)))

    (recur))
    (log/info "Stopped ingest stream."))

(defn main-send
  "Take chunks of Actions from the action-chan, assemble into Evidence Records,
   put them on the input-package-channel."
  []

  (loop []
    (try

      (evidence-log/log! {
        :i "a0029" :s agent-name :c "process" :f "start"})

      ; Take chunks of inputs, a few tweets per input bundle.
      ; Gather then into a Page of actions.
      (log/info "Waiting for chunks of actions...")
      (let [channel @action-chan
            ; We don't use any artifacts.
            artifact-map {}]
        (loop [actions (<!! channel)]
          (log/info "Got a chunk of" (count actions) "actions")

          (evidence-log/log! {
            :i "a0030" :s agent-name :c "process" :f "got-chunk" :v (count actions)})
          
          (let [evidence-record (assoc 
                                  (util/build-evidence-record manifest artifact-map)
                                  :pages [{:actions actions}])]

            (util/send-evidence-record manifest evidence-record)

            (log/info "Sent a chunk of" (count actions) "actions"))
          (recur (<!! channel))))

      (catch Exception ex (do
        (log/error "Unhandled exception" (.getMessage ex))
        (evidence-log/log! {
          :i "a003f" :s agent-name :c "process" :f "error"})))
      (finally
        (log/error "Stopped!")
        (Thread/sleep 1000)))

      (recur)))

(def manifest
  {:agent-name agent-name
   :source-id source-id
   :license util/cc-0
   :source-token source-token
   :schedule []
   :daemons [main-ingest-stream main-send]})

