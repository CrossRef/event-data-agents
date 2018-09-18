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
            [robert.bruce :refer [try-try-again]]
            [overtone.at-at :as at-at])
  (:import [java.net URLEncoder]
           [java.util UUID]
           [org.apache.commons.codec.digest DigestUtils]
           [javax.ws.rs.sse SseEventSource])
  (:gen-class))

(def source-token "36c35e23-8757-4a9d-aacf-345e9b7eb50d")
(def source-id "wikipedia")
(def agent-name "wikipedia-agent")

(def stream-url "https://stream.wikimedia.org/v2/stream/recentchange")

(def action-chunk-size
  "Number of input actions to batch into events. A value of 100 results in message size of about 150 KB"
  100)

(def action-input-buffer 1000000)

(def action-chan (delay (chan action-input-buffer (partition-all action-chunk-size))))

(declare manifest)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(defn message->action
  "Parse an input into a Percolator Action. Note that an Action ID is not supplied."
  [data]
  (when (= "edit" (:type data))
    (let [canonical-url (-> data :meta :uri)
          encoded-title (->> data :meta :uri (re-find #"^.*?/wiki/(.*)$") second)
          title (:title data)
          new-id (-> data :revision :new)
          version-url (str (:server_url data) "/w/index.php?title=" encoded-title "&oldid=" new-id)
          new-restbase-url (str (:server_url data) "/api/rest_v1/page/html/" encoded-title "/" new-id)

          ; normalize format
          timestamp-str (clj-time-format/unparse date-format (clj-time-coerce/from-string (-> data :meta :dt)))]

      {:url canonical-url
       :occurred-at timestamp-str
       :relation-type-id "references"
       :subj {:pid canonical-url
              :url version-url
              :title title
              :work_type_id "entry-encyclopedia"
              :api-url new-restbase-url}
       :observations [{:type :content-url
                       :input-url new-restbase-url
                       ; The Wikipedia robots.txt files prohibit access to the API.
                       ; There are separate terms for the RESTBase API, plus we have specific permission.
                       :ignore-robots true
                       :sensitive true}]
       :extra-events []})))

(def ignore-wikis
  "Set of wiki identifiers that we're not interested in."
  #{; Commons pages don't have references, so ignore them.
    "commonswiki"
    ; Wikidata uses a specific JSON format, which we can't yet parse. 
    "wikidatawiki"})

(def watchdog-timeout-ms
  10000)

(defn ingest-stream
  "Subscribe to the WC Stream, put data in the input-stream-chan.
  Blocks until there's a timeout or error."
  []
  (let [client (.build (javax.ws.rs.client.ClientBuilder/newBuilder))
        target (.target client stream-url)
        event-source (.build (javax.ws.rs.sse.SseEventSource/target target))
        last-event-timestamp (atom (System/currentTimeMillis))
        events-since-last-check (atom 0)
        wikis-breakdown (atom {})]

    (evidence-log/log! {:i "a0026" :s agent-name :c "wikipedia-api" :f "connect"})

    (def consumer (reify java.util.function.Consumer
                    (accept [this t]
                      (reset! last-event-timestamp (System/currentTimeMillis))
                      (let [data (.readData t)]
                        (when-not (clojure.string/blank? data)
                          (try
                            ; Read JSON, package into an Action.
                            (let [parsed (json/read-str (.readData t) :key-fn keyword)
                                  action (message->action parsed)
                                  wiki-identifier (:wiki parsed)]

                              ; Action may return nil if we're not interested in it.
                              (when (and action (not (ignore-wikis wiki-identifier)))
                                (swap! wikis-breakdown #(assoc % wiki-identifier (inc (% wiki-identifier 1))))
                                (swap! events-since-last-check inc)
                                (>!! @action-chan action)))

                            (catch Exception e
                              (do
                                (log/error "Error reading input" e)
                                (evidence-log/log! {:i "a003e" :s agent-name :c "ingest" :f "error"})))))))))

    (evidence-log/log! {:i "a0028" :s agent-name :c "ingest" :f "start"})

    (.register event-source ^java.util.function.Consumer consumer)
    (try
      (.open event-source)

        ; Block unless we run into a timeout.
      (loop []
        (let [num-events @events-since-last-check
              now (System/currentTimeMillis)
              diff (- now @last-event-timestamp)]

            ; There may be a little race, but this number is only an indicator.
          (reset! events-since-last-check 0)

          (log/info "Check timeout. Since last check got" num-events ", most recent" diff "ms ago")
          (log/debug "Wiki breakdown: " @wikis-breakdown)
          (reset! wikis-breakdown {})

          (if (> diff watchdog-timeout-ms)
            (do
              (log/warn "Timeout reached, closing connection")
              (.close event-source))
            (do
              (Thread/sleep 1000)
              (recur)))))

      (finally
        (log/warn "Interrupted. Closing connection.")
        (.close event-source)))))

(defn main-ingest-stream
  "Ingest RC Stream, reconnecting on error."
  []
  (loop []
    (ingest-stream)
    (log/error "Ingest stream exit. Will reconnect.")
    (recur)))

(defn main-send
  "Take chunks of Actions from the action-chan, assemble into Evidence Records,
   put them on the input-package-channel."
  []

  (loop []
    (try

      (evidence-log/log! {:i "a0029" :s agent-name :c "process" :f "start"})

      ; Take chunks of inputs, a few tweets per input bundle.
      ; Gather then into a Page of actions.
      (log/info "Waiting for chunks of actions...")
      (let [channel @action-chan
            ; We don't use any artifacts.
            artifact-map {}]
        (loop [actions (<!! channel)]
          (log/info "Got a chunk of" (count actions) "actions")

          (evidence-log/log! {:i "a0030" :s agent-name :c "process" :f "got-chunk" :v (count actions)})

          (let [evidence-record (assoc
                                 (util/build-evidence-record manifest artifact-map)
                                 :pages [{:actions actions}])]

            (util/send-evidence-record manifest evidence-record)

            (log/info "Sent a chunk of" (count actions) "actions"))
          (recur (<!! channel))))

      (catch Exception ex (do
                            (log/error "Unhandled exception sending:" (.getMessage ex))
                            (evidence-log/log! {:i "a003f" :s agent-name :c "process" :f "error"})))
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

