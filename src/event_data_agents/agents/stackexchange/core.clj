(ns event-data-agents.agents.stackexchange.core
  "StackExchange Agent has two modes:
    1 - scan of interested stackexchange sites (from the artifact). Regularly run as we expect Events,
    2 - retrieve all extant stackexchange sites and scan those (excluding those from the artifact). Irreguarly run as we don't expect to actually find anything."
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

            [clojurewerkz.quartzite.triggers :as qt]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.schedule.cron :as qc])
  (:import [java.util UUID]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.lang3 StringEscapeUtils])
  (:gen-class))

(def agent-name "stackexchange-agent")
(def source-id "stackexchange")
(def source-token "a8affc7d-9395-4f1f-a1fd-d00cfbdfa718")
(def user-agent "CrossrefEventDataBot (eventdata@crossref.org)")

(def version (System/getProperty "event-data-stackexchange-agent.version"))
(def api-host "https://api.stackexchange.com")

(declare manifest)

(def excerpts-filter
  "Stable filter string that specifies the fields we want to get back from excerpt search."
  "!FcbKgRXe3Y.kh-SxIte4x1.ZCx")

(def sites-filter
  "Stable filter string that specifies the fields we want to get back from sites list."
  "!)Qpa1bGM9MgBBV.BJ1yrJ8GF")

; https://api.stackexchange.com/docs/paging
(def page-size 100)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(defn fetch-sites-page
  "Fetch an API result, return a page of Sites."
  [page-number]
  (log/info "Fetch list of sites")
  (let [url (str api-host "/2.2/sites" )
        query-params {:page page-number
                      :filter sites-filter
                      :pagesize page-size}]
    
    (evidence-log/log! {
      :i "a0018" :s agent-name :c "stackexchange-api" :f "retrieve-all-sites-request"
      :p page-number :u url})

    ; If the API returns an error
    (try
      (try-try-again
        {:sleep 30000 :tries 10}
        #(let [result (client/get url {:headers {"User-Agent" user-agent} :query-params query-params :throw-exceptions true})]

          (log/info "Fetched" url query-params)

          (evidence-log/log! {
            :i "a0019" :s agent-name :c "stackexchange-api" :f "retrieve-all-sites-response"
            :u url :e (:status result)})

          (condp = (:status result)
            200 (json/read-str (:body result) :key-fn keyword)
            
            ; Not found? Stop.
            404 {:url url :actions [] :extra {:error "Result not found"}}

            ; Don't throw on this exception, retrying won't help.
            400 {:url url :actions [] :extra {:error "Bad Request, maybe rate limit"}}
            
            (do
              (log/error "Unexpected status code" (:status result) "from" url)
              (log/error "Body of error response:" (:body url))
              (throw (new Exception "Unexpected status"))))))

      (catch Exception ex (do
        (evidence-log/log! {
          :i "a0019" :s agent-name :c "stackexchange-api" :f "error"})

        (log/error "Error fetching" url)
        (log/error "Exception:" ex)
        {:url url :items [] :extra {:error "Failed to retrieve page"}})))))

(defn fetch-sites
  ([] (fetch-sites 1))
  ([page-number]
    (let [result (fetch-sites-page page-number)
          items (result :items [])]
      (if (result :has_more)
        (lazy-cat items (fetch-sites (inc page-number)))
        items))))

(defn api-item-to-action
  [item site-url]
  (let [typ (:item_type item)
        creation-date (clj-time-format/unparse date-format (coerce/from-long (* 1000 (long (:creation_date item)))))
        question-id (:question_id item)
        answer-id (:answer_id item)
        url (condp = typ
              "question" (str site-url "/q/" question-id)
              "answer" (str site-url "/a/" answer-id)
              nil)

        work-type (condp = typ
              "question" "webpage"
              "answer" "comment"
              nil)

        author-name (-> item :owner :display_name)
        author-id (-> item :owner :user_id)
        author-url (-> item :owner :link)

        ; text comes encoded
        text (StringEscapeUtils/unescapeHtml4 (:body item ""))]

  ; URL only constructed when we get a type we recognise.
  (when url
    {:url url
     :relation-type-id "discusses"
     ; as this comes from the specific API, don't use a general purpose URL as the action id.
     :id (DigestUtils/sha1Hex ^String (str "stackexchange-" url))
     :occurred-at creation-date
     :subj {
       :pid url
       :title (StringEscapeUtils/unescapeHtml4 (:title item ""))
       :issued creation-date
       :type work-type
       :author {:url author-url :name author-name :id author-id}}
     :observations [{:type :plaintext
                    :input-content text
                    :sensitive true}]})))

; API
(defn parse-page
  "Parse response JSON to a page of Actions."
  [url site-url json-data]
  (let [parsed (json/read-str json-data :key-fn keyword)
       items (:items parsed)]
    {:url url
     :extra (select-keys parsed [:quota_remaining :quota_max :backoff])
     :actions (map #(api-item-to-action % site-url) items)}))

(defn fetch-page
  "Fetch the API result, return a page of Actions."
  [evidence-record-id site-info domain from-date page-number]

  (log/info "Fetch page for site" site-info "domain" domain)
  (let [url (str api-host "/2.2/search/excerpts" )
        query-params {:order "desc"
                      :sort "creation"
                      :q (str "url:\"" domain "\"")
                      :fromdate from-date
                      :page page-number
                      :pagesize page-size
                      :site (:api_site_parameter site-info)
                      :filter excerpts-filter}]

    (evidence-log/log! {
      :i "a001a" :s agent-name :c "stackexchange-api" :f "search-request"
      :v domain :p page-number
      :r evidence-record-id })

    ; If the API returns an error
    (try
      (try-try-again
        {:sleep 30000 :tries 10}
        #(let [result (client/get url {:headers {"User-Agent" user-agent} :query-params query-params :throw-exceptions true})]

          (log/info "Fetched" url query-params)

          (evidence-log/log! {
            :i "a001b" :s agent-name :c "stackexchange-api" :f "search-response"
            :r evidence-record-id :v domain :p page-number
            :e (:status result)})

          (condp = (:status result)
            200 (parse-page url (:site_url site-info) (:body result))
            
            ; Not found? Stop.
            404 {:url url :actions [] :extra {:error "Result not found"}}

            ; Don't throw on this exception, retrying won't help.
            400 {:url url :actions [] :extra {:error "Bad Request, maybe rate limit"}}
            
            (do
              (log/error "Unexpected status code" (:status result) "from" url)
              (log/error "Body of error response:" (:body url))
              (throw (new Exception "Unexpected status"))))))

      (catch Exception ex (do
        (log/error "Error fetching" url)
        (log/error "Exception:" ex)

        (evidence-log/log! {
            :i "a001c" :s agent-name :c "stackexchange-api" :f "search-error"
            :r evidence-record-id :v domain :p page-number})

        {:url url :actions [] :extra {:error "Failed to retrieve page"}})))))

; https://api.stackexchange.com/docs/throttle
; Not entirely predictable. Go ultra low. 
(def fetch-page-throttled (throttle-fn fetch-page 5 :hour))

(defn fetch-pages
  "Lazy sequence of pages for the domain."
  ([evidence-record-id site-info domain from-date]
    ; Pagination starts with 1! 
    ; https://api.stackexchange.com/docs/paging
    (fetch-pages evidence-record-id site-info domain from-date 1))

  ([evidence-record-id site-info domain from-date page-number]
    (log/info "Query page" page-number "of" site-info domain)
    (let [result (fetch-page-throttled evidence-record-id site-info domain from-date page-number)
          end (-> result :extra :has_more not)
          ; as float or nil
          quota-remaining-proportion (when (and (-> result :extra :quota_max)
                                       (-> result :extra :quota_remaining)
                                       (> (-> result :extra :quota_max) 0))
                                     (float (/ (-> result :extra :quota_remaining)
                                               (-> result :extra :quota_max))))

          backoff-seconds (-> result :extra :backoff)

                             ; We get 400s on over-quota requests.
          emergency-stop (or (-> result :exta :error)
                             (-> result :extra :quota_remaining (or 0) zero?))]

      (log/info "Quota remaining:" quota-remaining-proportion "," (-> result :extra :quota_remaining) "/" (-> result :extra :quota_max) "quota remaining!")

      ; In last 10% of quota, sleep more between requests.
      (when (and quota-remaining-proportion (< quota-remaining-proportion 0.1))
        (log/info "Warning! " (-> result :extra :quota_remaining) "/" (-> result :extra :quota_max) "quota remaining!")
        (Thread/sleep 20000))

      (when backoff-seconds
        (log/info "Back off for" backoff-seconds "seconds")
        (Thread/sleep (* 1000 backoff-seconds)))

      (when emergency-stop
        (log/error "Out of API quota, stopping before end of results!"))

      (if (or end emergency-stop)
        [result]
        (lazy-seq (cons result (fetch-pages evidence-record-id site-info domain from-date (inc page-number))))))))

(defn scan-sites
  "Check all mentioned sites for unseen links. Send an Evidence Record per site.
   Sites is a seq of site-infos with keys {:site_url :api_site_parameter}."

  [artifact-map site-infos cutoff-date]
  (let [site-counter (atom 0)
        num-sites (count site-infos)]

    (doseq [site-info site-infos]
      (swap! site-counter inc)
      (log/info "Query site" (:site_url site-info) @site-counter "/" num-sites " total progress " (int (* 100 (/ @site-counter num-sites))) "%")
      (let [base-record (util/build-evidence-record manifest artifact-map)
            evidence-record-id (:id base-record)
            ; API takes timestamp.
            from-date (int (/ (coerce/to-long cutoff-date) 1000))

            ; Realize lazy seq to ensure it doesn't get realized inside JSON serialization buried in Kafka Producer submission.
            pages (doall (fetch-pages evidence-record-id site-info "doi.org" from-date))
            evidence-record (assoc base-record
                              :extra {:cutoff-date (str cutoff-date) :queried-domain "doi.org"}
                              :pages pages)]
        
        (log/info "Sending evidence-record" (:id evidence-record) "...")
        
        (util/send-evidence-record manifest evidence-record)

        (log/info "Sent evidence-record" (:id evidence-record) "."))))
  (log/info "Finished scan."))


(defn main-sites-from-artifact
  "Check all sites for unseen links."
  []

  (evidence-log/log! {
    :i "a001d" :s agent-name :c "scan-selected-sites" :f "start"})

  (log/info "Start crawl all Sites from artifact at" (str (clj-time/now)))
  (let [artifact-map (util/fetch-artifact-map manifest ["stackexchange-sites"])
        [_ site-list] (artifact-map "stackexchange-sites")
        
        ; Sites artifact is {:site_url :api_site_parameter}
        sites (json/read-str site-list :key-fn keyword)
        cutoff-date (-> 40 clj-time/days clj-time/ago)]

    (scan-sites artifact-map sites cutoff-date)

    (evidence-log/log! {
    :i "a001e" :s agent-name :c "scan-selected-sites" :f "finish"})))


(defn main-all-sites
  "Check all sites (except those in artifact, as we scan those on a more regular basis.)"
  []
  (evidence-log/log! {
    :i "a001f" :s agent-name :c "scan-all-sites" :f "start"})

  (log/info "Start crawl all Sites on stackexchange at" (str (clj-time/now)))
  (let [artifact-map (util/fetch-artifact-map manifest ["stackexchange-sites"])
        [_ site-list] (artifact-map "stackexchange-sites")
        
        ; Sites artifact is [{:site_url :api_site_parameter}]
        artifact-sites (map #(select-keys % [:site_url :api_site_parameter]) (json/read-str site-list :key-fn keyword))
        all-sites (map #(select-keys % [:site_url :api_site_parameter]) (fetch-sites))

        sites-not-in-artifact (clojure.set/difference (set all-sites) (set artifact-sites))

        num-sites (count sites-not-in-artifact)
        site-counter (atom 0)

        cutoff-date (-> 40 clj-time/days clj-time/ago)]

    (log/info "Found" (count artifact-sites) "sites in artifact,"
              (count all-sites) "possible available. Scanning difference" num-sites "sites")

    (scan-sites artifact-map sites-not-in-artifact cutoff-date)

    (evidence-log/log! {
      :i "a0020" :s agent-name :c "scan-all-sites" :f "finish"})))

(defjob main-all-sites-job
  [ctx]
  (main-all-sites))

(defjob main-sites-from-artifact-job
  [ctx]
  (main-sites-from-artifact))

(def main-all-sites-trigger
  (qt/build
    (qt/with-identity (qt/key "stackexchange-all-sites"))
    (qt/start-now)
    (qt/with-schedule (qc/cron-schedule "0 0 0 */5 * ?"))))

(def main-sites-from-artifact-trigger
  (qt/build
    (qt/with-identity (qt/key "stackexchange-all-sites"))
    (qt/start-now)
    (qt/with-schedule (qc/cron-schedule "0 0 0 1 * ?"))))

(def manifest
  {:agent-name agent-name
   :source-id source-id
   :license util/cc-0
   :source-token source-token
   :schedule [[(qj/build (qj/of-type main-all-sites-job)) main-all-sites-trigger]
              [(qj/build (qj/of-type main-sites-from-artifact-job)) main-sites-from-artifact-trigger]]
   :runners []})

