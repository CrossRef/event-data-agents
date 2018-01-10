(ns event-data-agents.agents.carberry.core
  "For demo / testing. Send a single Josiah Carberry event.
   No checkpointing, sends the same event every time"

  (:require [event-data-agents.util :as util]
            [event-data-common.evidence-log :as evidence-log]
            [crossref.util.doi :as cr-doi]
            [clojure.tools.logging :as log]
            [clojure.java.io :refer [reader]]
            [clojure.data.json :as json]
            [clj-time.coerce :as coerce]
            [clj-time.core :as clj-time]
            [throttler.core :refer [throttle-fn]]
            [clj-http.client :as client]
            [config.core :refer [env]]
            [robert.bruce :refer [try-try-again]]
            [clj-time.format :as clj-time-format]
  (:gen-class)))

(def agent-name "carberry-agent")
(def source-token "8a802f32-63bc-4b5c-a14d-ed56a3393ffd")

(declare manifest)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(defn main
  []
  (let [base (util/build-evidence-record manifest {})
        record (assoc base :pages [
          {:actions 
            [{:url "https://www.crossref.org/blog/doi-like-strings-and-fake-dois/"
              :relation-type-id "references"
              :subj {}
              :occurred-at (clj-time-format/unparse date-format (clj-time/now))
              :observations [{:type "plaintext" :input-content "see 10.5555/12345678" :sensitive false}]}]}])]
    (util/send-evidence-record manifest record)))

(def manifest
  {:agent-name agent-name
   :source-id "carberry"
   :license util/cc-0
   :source-token source-token
   :schedule [[main (clj-time/minutes 1)]]
   :runners []})
