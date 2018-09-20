(ns event-data-agents.util
  "Utility function available to Agents."
  (:require [event-data-common.jwt :as jwt]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [robert.bruce :refer [try-try-again]]
            [event-data-common.artifact :as artifact]
            [event-data-common.backoff :as backoff]
            [event-data-common.evidence-log :as evidence-log]
            [clojure.core.async :refer [go-loop thread buffer chan <!! >!! >! <!]])

  (:import [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord]
           [java.util UUID])
  (:gen-class))

(def version
  "Version of the whole codebase. Individual Agents report this version."
  (System/getProperty "event-data-agents.version"))

(def http-user-agent
  "User Agent for HTTP requests.
  Some Agents (e.g. Reddit) use a slightly different, more specific one."

  "CrossrefEventDataBot (eventdata@crossref.org)")

; Licenses.
(def cc-0 "https://creativecommons.org/publicdomain/zero/1.0/")
(def cc-by-sa4 "https://creativecommons.org/licenses/by-sa/4.0/")

(def kafka-producer
  (delay
   (KafkaProducer.
    {"bootstrap.servers" (:global-kafka-bootstrap-servers env)
     "acks" "1"
     "retries" (int 5)
     "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
     "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})))

(def date-format
  (clj-time-format/formatters :basic-date))

(def jwt-verifier
  "A JWT verifier for signing and verifying JWTs using the configured secret."
  (delay (jwt/build (:global-jwt-secrets env))))

(defn build-evidence-record
  "Generate a standard base Evidence Record. Generate an ID based on agent and timestamp.
   Supply Agent Manifest and an Artifact Map."
  ([manifest artifacts extra]
   (merge extra (build-evidence-record manifest artifacts)))
  ([manifest artifacts]
   (let [now (clj-time/now)
         source-id (:source-id manifest)
         id (str
             (clj-time-format/unparse date-format now)
             "-" source-id "-"
             (UUID/randomUUID))
         now-str (str now)]
     {:id id
      :source-id (:source-id manifest)
      :source-token (:source-token manifest)
      :timestamp now-str

       ; Transform {artifact-name [version-url text-content]}
       ; into {artifact-name version-url}
      :artifacts (into {} (map #(vector (first %) (-> % second first)) artifacts))
      :agent {:version version
              :name (:agent-name manifest)}
      :license (:license manifest)})))

(defn send-evidence-record
  "Send an Evidence Record into the Kafka queue."
  [manifest evidence-record]
  ; If Kafka isn't set up, just log these. WARN to stop this happening by mistake.
  (if-not (:global-kafka-bootstrap-servers env)
    (log/warn "Send Evidence Record:" evidence-record)
    (do
    ; Agents might send nil or empty evidence records (e.g. Newsfeed on an error).
    ; Guarding here gives broadest protection.
    (when (not-empty evidence-record)
      (let [; Add a JWT so that the Percolator knows that the message is from us.
            with-jwt (assoc evidence-record
                            :jwt (jwt/sign @jwt-verifier {"sub" (:source-id manifest)}))
            topic (:percolator-input-evidence-record-topic env)
            id (:id evidence-record)]

        (evidence-log/log! {:i "a0001" :s (:agent-name manifest) :c "evidence" :f "send" :r id})

        (log/info "Send" (:id evidence-record) "to" topic)
        (try
          (let [result (.send @kafka-producer (ProducerRecord. topic
                                                               id
                                                               (json/write-str with-jwt)))]

            ; Wait for the future, so to speak.
            (.get result))

          (catch java.util.concurrent.ExecutionException ex
            (do (log/error "Failed to send Evidence Record to Kafka:" id)
                ; Of course if Kafka is down, Evidence Logging may also fail. 
                (evidence-log/log! {:i "a0040" :s (:agent-name manifest) :c "evidence" :f "send-error" :r id})))))))))

(defn fetch-artifact-map
  "From a seq of Artifact names, fetch {artifact-name [version-url text-content]}.
   Also log to evidence log."
  [manifest artifact-names]
  (into {} (map (fn [artifact-name]
                  (let [version-url (artifact/fetch-latest-version-link artifact-name)
                        content (artifact/fetch-latest-artifact-string artifact-name)]

                    (evidence-log/log!
                     {:i "a0002"
                      :s (:agent-name manifest)
                      :c "artifact"
                      :f "fetch"
                      :v artifact-name
                      :u version-url})

                    [artifact-name [version-url content]]))
                artifact-names)))

