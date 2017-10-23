(ns event-data-agents.agents.hypothesis.core
  "Process every Hypothes.is annotation.

   Schedule and checkpointing:
   Connect to Hypothes.is API every S hours and get all results since the last run."
  (:require [event-data-agents.util :as util]
            [event-data-agents.checkpoint :as checkpoint]
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

(def agent-name "hypothesis-agent")
(def source-token "8075957f-e0da-405f-9eee-7f35519d7c4c")
(def api-url "https://hypothes.is/api/search")

(declare manifest)

; Max according to API docs.
(def page-size 200)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(defn api-item-to-actions
  [item]
  ; Return two Actions with different observation types:
  ; - one that says "this Annotation is about this DOI"
  ; - another that says "this Annotation has this text, which may contain DOIs"
  ; Both or neither may match to zero, one or more matches.
  (let [occurred-at-iso8601 (clj-time-format/unparse date-format (coerce/from-string (:updated item)))]
        
    ; ID is legacy from first version where there was only one kind of action per annotation.
    [{:id (str "hypothesis-" (:id item))
      :url (-> item :links :html)
      :relation-type-id "annotates"
      :occurred-at occurred-at-iso8601
      :observations [{:type :url :input-url (-> item :uri)}]
      :extra {}
      :subj {
       :json-url (-> item :links :json)
       :pid (-> item :links :html)
       :url (-> item :links :incontext)
       :alternative-id (:id item)
       :type "annotation"
       :title (-> item :text)
       :issued occurred-at-iso8601}}

     {:id (str "hypothesis-" (:id item) "-text")
      :url (-> item :links :html)
      :relation-type-id "discusses"
      :occurred-at occurred-at-iso8601
      :observations [{:type :plaintext :input-content (-> item :text)}]
      :extra {}
      :subj {
       :json-url (-> item :links :json)
       :pid (-> item :links :html)
       :url (-> item :links :incontext)
       :alternative-id (:id item)
       :type "annotation"
       :title (-> item :text)
       :issued occurred-at-iso8601}}]))

; API
(defn parse-page
  "Parse response JSON to a page of Actions."
  [url json-data]
  (let [parsed (json/read-str json-data :key-fn keyword)]
    {:url url
     :actions (mapcat api-item-to-actions (-> parsed :rows))}))

(defn fetch-evidence-record-page
  "Fetch a page of results, embedded in an Evidence Record.
   Evidence Record created for each page so that there's an Evidence Record ID
   associcated with the HTTP Request for tracing."

  [offset]
  (let [base-record (util/build-evidence-record manifest {})
        evidence-record-id (:id base-record)
        page (try

               (evidence-log/log!
                 {:i "a0003" :s agent-name :c "hypothesis-api" :f "request"
                  :r evidence-record-id :v offset})
               
               (try-try-again
                 {:sleep 30000 :tries 10}
                 #(let [result (client/get
                                 api-url
                                 {:query-params {
                                   :offset offset
                                   :limit page-size
                                   :sort "updated"
                                   :order "desc"}
                                  :headers {"User-Agent" util/http-user-agent}})]
 
                   (log/info "Fetched page" {:offset offset :limit page-size})
 
                   (evidence-log/log!
                     {:i "a0004" :s agent-name :c "hypothesis-api" :f "response"
                      :e (:status result) :r evidence-record-id})
 
                   (condp = (:status result)
                     200 (parse-page api-url (:body result))
                     404 {:url api-url :actions [] :extra {:after nil :before nil :error "Result not found"}}
                     
                     (do
                       (log/error "Unexpected status code" (:status result) "from" api-url)
                       (log/error "Body of error response:" (:body result))
                       (throw (new Exception "Unexpected status"))))))
 
               (catch Exception ex (do
 
                 (evidence-log/log!
                   {:i "a0005" :s agent-name :c "hypothesis-api" :f "error" :r evidence-record-id})
 
                 (log/error "Error fetching" api-url)
                 (log/error "Exception:" ex)
                 {:url api-url :actions [] :extra {:error "Failed to retrieve page"}})))]

    (assoc base-record :pages [page])))

(def fetch-evidence-record-page-throttled (throttle-fn fetch-evidence-record-page 20 :minute))

(defn fetch-evidence-record-pages
  "Lazy sequence of pages until we get no more results."
  ([] (fetch-evidence-record-pages 0))
  ([offset]
    (log/info "fetch page offset" offset)
    (let [evidence-record-page (fetch-evidence-record-page-throttled offset)
          actions (-> evidence-record-page :pages first :actions)]

      (log/info "Got" (count actions) "actions")

      (if (empty? actions)
        [evidence-record-page]
        (lazy-seq (cons evidence-record-page
                        (fetch-evidence-record-pages (+ offset page-size))))))))

(defn all-action-dates-after?
  "Are all the action dates in the Evidence Record after the given date?"
  [date evidence-record]
  (let [dates (map #(-> % :occurred-at coerce/from-string)
                    (-> evidence-record :pages first :actions))]
    (every? #(clj-time/after? % date) dates)))

(defn fetch-parsed-evidence-record-pages-after
  "Fetch seq parsed pages of Actions until all actions on the page happened before the given time."
  [date]
  (let [pages (fetch-evidence-record-pages)]
    (take-while (partial all-action-dates-after? date) pages)))

(defn scan
  [cutoff-date]
  (log/info "Start scan all hypothesis at" (str (clj-time/now)) "with cutoff date:" (str cutoff-date))

  (evidence-log/log! {:i "a0006" :s agent-name :c "scan" :f "start"})

  (let [checkpoint-id ["hypothesis" "checkpoint-scan"]
        evidence-records (fetch-parsed-evidence-record-pages-after cutoff-date)]

    (doseq [evidence-record evidence-records]
      (log/info "Sending evidence record...")
      (util/send-evidence-record manifest evidence-record)))

  (evidence-log/log! {:i "a0007" :s agent-name :c "scan" :f "finish"})

  (log/info "Finished scan."))

(defn main
  "Main function for Hypothesis agent."
  []
  (checkpoint/run-checkpointed!
    ["hypothesis" "all-scan"]
    (clj-time/hours 2) ; Maximum of once every 2 hours.
    (clj-time/years 5) ; Look back at most 5 years.
    scan))

(def manifest
  {:agent-name agent-name
   :source-id "hypothesis"
   :license util/cc-0
   :source-token source-token
   ; No need to leave less than an hour between scans.
   :schedule [[main (clj-time/hours 1)]]
   :runners []})
