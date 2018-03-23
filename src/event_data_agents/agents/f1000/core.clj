(ns event-data-agents.agents.f1000.core
  "Links from F1000 reviews to DOIs.

  F1000 provides an export of articles that they review, and reviews for them. The file always grows, and isn't
  log structured beause any article could have a new review added to it. 

  We can filter out Reviews that happened before the last checkpoint, plus some overlap.
  Also rely on the Percolator's Action deduping to remove any overlap.

  Schedule and checkpointing:
   - Run every month.
   - Parse the dump from F1000. Filter out those reviews published since the last run date."
  (:require [event-data-agents.util :as util]
            [crossref.util.doi :as cr-doi]
            [event-data-common.checkpoint :as checkpoint]
            [event-data-common.evidence-log :as evidence-log]
            [clojure.tools.logging :as log]
            [clj-time.coerce :as coerce]
            [clj-time.core :as clj-time]
            [clj-http.client :as client]
            [config.core :refer [env]]
            [clj-time.format :as clj-time-format]
            ; [clojure.xml :as c-xml]
            [clojure.data.xml :as c-d-xml :refer [parse]]
            [clojure.zip :as zip :refer [xml-zip]]
            ; [clojure.data.zip :as c-d-zip]
            [clojure.data.zip.xml :as z-xml]
            [clojure.java.io :as io])
  (:import [org.apache.commons.codec.digest DigestUtils])
  (:gen-class))

(def source-token "85dce7cf-7bbf-40c0-a983-739a784db09a")
(def source-id "f1000")
(def agent-name "f1000-agent")

(def page-size
     "Number of actions to include in each Evidence Record"
     100)

(declare manifest)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(defn actions-from-article
  "Produce a lazy sequence of all Actions from an Article entry. 
   Filter to include only Actions after this datetime."
  [after-time article-element]
  
  (let [article (zip/xml-zip article-element)
        
        obj-doi (cr-doi/normalise-doi (z-xml/xml1-> article :Doi z-xml/text))
        obj-pubmed-id (z-xml/xml1-> article :PubmedId z-xml/text)
        obj-alternative-id (z-xml/xml1-> article :Id z-xml/text)

        ; The three attributes of the subject review are present in three parallel sequences. 
        subjs-alternative-ids (map #(-> % zip/xml-zip z-xml/text)
                                    (z-xml/xml-> article :RecommendationIDs zip/children))

        subjs-dates (map #(-> % zip/xml-zip z-xml/text)
                                    (z-xml/xml-> article :RecommendationDates zip/children))

        subjs-dois (map #(-> % zip/xml-zip z-xml/text)
                                    (z-xml/xml-> article :RecommendationDOIs zip/children))]
    (filter identity
            (map
              (fn [subj-alternative-id subj-date-str subj-doi]

                (let [subj-id (cr-doi/normalise-doi subj-doi)
                      obj-id (cr-doi/normalise-doi obj-doi)
                      occurred-at (coerce/from-string subj-date-str)
                      occurred-at-iso8601 (clj-time-format/unparse date-format occurred-at)]
                  (when (clj-time/after? occurred-at after-time)
                    
                    ; Dedupe by the subj-obj pair, as each Article can have Reviews added at any point in time.
                    {:id (DigestUtils/sha1Hex ^String (str "f1000-" subj-alternative-id "-" obj-alternative-id))
                     :relation-type-id "reviews"
                     :occurred-at occurred-at-iso8601
                     :url subj-id
                     :subj {:pid subj-id
                            :alternative_id subj-alternative-id
                            :work_type_id "review"}

                     :observations [{:type :url :input-url obj-id}]})))
              
              subjs-alternative-ids
              subjs-dates
              subjs-dois))))

(defn actions-from-stream
  "Produce a lazy sequence of all Events from a given dump.
   Filter out events after the given time."
  [xml-reader after-time]
  (let [data (parse xml-reader)
        articles (-> data zip/xml-zip zip/children)]
    (mapcat (partial actions-from-article after-time) articles)))

(defn main-fetch
  [last-date]
  (with-open [http-stream (io/reader (:body (client/get (:f1000-dump-path env) {:as :stream})))]
    (log/info "Filter Actions after" last-date)
    
    (let [actions (actions-from-stream http-stream last-date)
          pages (partition-all page-size actions)]
      (doseq [actions pages]
        (let [evidence-record (assoc
                                (util/build-evidence-record manifest {})
                                :pages [{:actions actions}])]
          (util/send-evidence-record manifest evidence-record)
          (log/info "Sent a chunk of" (count actions) "actions"))))

  (log/info "Finished sending Actions")))

(defn main
  "Scan dump."
  []
  (log/info "Start F1000 fetch at " (str (clj-time/now)))

  (evidence-log/log! {:i "a0041" :s agent-name :c "scan" :f "start"})

  ; Check only every 10 days.
  (checkpoint/run-checkpointed!
    ["f1000" "fetch-dump"]
    (clj-time/days 10) 
    (clj-time/years 1000)
    main-fetch)

  (evidence-log/log! {:i "a0042" :s agent-name :c "scan" :f "finish"})
  (log/info "Finished scan."))

(def manifest
  {:agent-name agent-name
   :source-id source-id
   :license util/cc-0
   :source-token source-token
   :schedule [[main (clj-time/days 1)]]
   :runners []})

