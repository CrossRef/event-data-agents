(ns event-data-agents.checkpoint
  "Checkpoint functionality for Agents.
   Allows setting of checkpoints and function to check if the checkpoint happened more than a period of time ago.
   Checkpoint identifiers are anything that can be converted to a string, and are hashed.

   This is used so that Agents can run in continual loops, only re-trying when a given time period has elapsed,
   choosing which items to checkpoint on (e.g. per whole domain scan, or per newsfeed). 

   Doing it this way makes it possible to resume a scan in the face of a crash. It also means that we can add new
   domains in future, and the Reddit source could back-scan for that domain, regardless of others.
   
   Checkpoints are stored as JSON in case we want to inspect them later.
   Longs stored as Strings because you can't really trust numbers in JSON."
  (:require [event-data-common.storage.s3 :as s3]
            [event-data-common.storage.store :as store]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [config.core :refer [env]]
            [clj-time.core :as clj-time]
            [clj-time.coerce :as clj-time-coerce]
            [robert.bruce :refer [try-try-again]]
            [event-data-common.artifact :as artifact]
            [event-data-common.backoff :as backoff]
            [event-data-common.evidence-log :as evidence-log]
            [clojure.data.json :as json]
            [clojure.core.async :refer [go-loop thread buffer chan <!! >!! >! <!]])

  (:import [org.apache.commons.codec.digest DigestUtils])
  (:gen-class))

(def checkpoint-store
  (delay
   (s3/build
    (:agent-checkpoint-s3-key env)
    (:agent-checkpoint-s3-secret env)
    (:agent-checkpoint-s3-region-name env)
    (:agent-checkpoint-s3-bucket-name env))))

(defn hash-identifier
  [identifier]
  (str "checkpoint/" (DigestUtils/sha1Hex ^String (str identifier))))

(defn get-checkpoint
  "For a given checkpoint, return the date time that it happened, or nil"
  [identifier]
  (let [hashed-identifier (hash-identifier identifier)
        result (store/get-string @checkpoint-store hashed-identifier)]
    (when result
      (let [parsed (json/read-str result :key-fn keyword)]
        (clj-time-coerce/from-long (Long/parseLong (:timestamp-str parsed)))))))

(defn floor-date
  "If the supplied date is nil or earlier than the time-period, return the time-period-ago date."
  [date ^org.joda.time.ReadablePeriod period-ago]
  (let [date-ago (clj-time/minus (clj-time/now) period-ago)]
    ; If it's null, return date-ago.
    (if-not date
      date-ago
      ; Else return later date.
      (if (clj-time/before? date-ago date)
        date
        date-ago))))

(defn get-floored-checkpoint-date
  "Get the checkpoint date, limited to maximum period-ago."
  [identifier period-ago]
  (floor-date (get-checkpoint identifier) period-ago))

(defn has-time-elapsed?
  "Has at least the given time elapsed since the last checkpoint for this identifier?
   If so, return the last date. If not, return nil."
  [identifier ^org.joda.time.ReadablePeriod period-ago]
  (let [checkpoint-date (get-checkpoint identifier)
        date-ago (clj-time/ago period-ago)]
    (if-not checkpoint-date
      ; Time has always elapsed since the beginning of time.
      true
      (if (clj-time/after? checkpoint-date date-ago)
        ; If the checkpoint happened more recently than the time-ago, then nil.
        nil
        ; Otherwise, return that date
        checkpoint-date))))

(defn set-checkpoint!
  "Set checkpoint with identifier to given time, or now."
  ([identifier] (set-checkpoint! identifier (clj-time/now)))
  ([identifier time-value]
   (let [hashed-identifier (hash-identifier identifier)]
     (store/set-string
      @checkpoint-store
      hashed-identifier
      (json/write-str {:identifier identifier
                       :hash-identifier hashed-identifier
                       :timestamp-human (str time-value)
                       ; ONLY timestamp-str is used. The rest are for diagnostic / debug purposes.
                       :timestamp-str (str (clj-time-coerce/to-long time-value))})))))

(defn run-checkpointed!
  "Run the given function and set checkpoint with identifier if there hasn't been a checkpoint in the given time period.
   Call function with date of last run, or floor-period-ago if it never ran before"
  [identifier since-period-ago floor-period-ago function]

  ; This may be long-running. Checkpoint the time we started the activity, not the time we finished.
  (let [starting-time (clj-time/now)
        last-checkpoint-date (has-time-elapsed? identifier since-period-ago)]

    (when-not last-checkpoint-date
      (log/info "Checkpoint skipping" identifier))

    (when last-checkpoint-date
      ; NB last-checkpoint-date can be nil, true or a date.
      (log/info "Checkpoint running" identifier "last time:" (str last-checkpoint-date))

      ; This is often called directly in a pmap, where the threadpool can swallow exceptions.
      (try
        ; We either get a last-run date or a 'true' if never run before. In this case, just turn it into nil.
        (if (= true last-checkpoint-date)
          (function (clj-time/ago floor-period-ago))
          (function last-checkpoint-date))

        (catch Exception ex
          (do (log/error "Error processing" function "with checkpoint" identifier)
              (.printStackTrace ex))))

      (set-checkpoint! identifier starting-time))))

