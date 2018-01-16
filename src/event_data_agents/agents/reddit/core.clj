(ns event-data-agents.agents.reddit.core
  "Query Reddit for posts that mention every domain.

  Schedule and checkpointing:
  Continually loop over the domain list.
  Check for each domain no more than once per C."
  (:require [event-data-agents.util :as util]
            [event-data-agents.checkpoint :as checkpoint]
            [event-data-common.evidence-log :as evidence-log]
            [event-data-common.url-cleanup :as url-cleanup]
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
            [clj-time.format :as clj-time-format])
  (:import [org.apache.commons.codec.digest DigestUtils])
  (:gen-class))

(def agent-name "reddit-agent")
(def source-id "reddit")
(def source-token "a6c9d511-9239-4de8-a266-b013f5bd8764")

(def user-agent
  "HTTP User Agent. A bit more Reddit-specific detail added."
  "CrossrefEventDataBot (eventdata@crossref.org) (by /u/crossref-bot labs@crossref.org)")

(declare manifest)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

; Auth
(def reddit-token (atom nil))
(defn fetch-reddit-token
  "Fetch a new Reddit token."
  [evidence-record-id]
  (let [response (client/post
                  "https://www.reddit.com/api/v1/access_token"
                  {:as :text
                   :headers {"User-Agent" user-agent}
                   :form-params {"grant_type" "password"
                                 "username" (:reddit-app-name env)
                                 "password" (:reddit-password env)}
                   :basic-auth [(:reddit-client env) (:reddit-secret env)]
                   :throw-exceptions false})
        token (when-let [body (:body response)]
                (->
                 (json/read-str body :key-fn keyword)
                 :access_token))]

    (evidence-log/log!
     {:i "a000e"
      :s agent-name
      :c "reddit-api"
      :f "authenticate"
      :r evidence-record-id
      :e (if token "t" "f")})

    token))

(defn renew-reddit-token
  "Reset the auth token with a new one."
  [evidence-record-id]
  (reset! reddit-token (fetch-reddit-token evidence-record-id)))

; https://www.reddit.com/dev/api/
(def work-types
  "Mapping of reddit object types to lagotto work types. Only expect ever to see t1 and t3.
  In any case, events only get this far if they have a URL that matched a DOI."

  {"t1" "personal_communication" ; comment
   "t2" "webpage" ; Account
   "t3" "post" ; Link
   "t4" "personal_communication"; Message
   "t5" "webpage" ; Subreddit
   "t6" "webpage" ; Award
   "t8" "webapge" ; PromoCampaign
})

(defn api-item-to-action
  [item]
  (let [occurred-at-iso8601 (clj-time-format/unparse date-format (coerce/from-long (* 1000 (long (-> item :data :created_utc)))))]
    {:id (DigestUtils/sha1Hex ^String (str "reddit-" (-> item :data :id)))
     :url (str "https://reddit.com" (-> item :data :permalink))
     :relation-type-id "discusses"
     :occurred-at occurred-at-iso8601
     :observations [{:type :url
                     :input-url (-> item
                                    :data
                                    :url
                                    ; Remove tracking parameters at this point.
                                    url-cleanup/remove-tracking-params)}]
     :extra {:subreddit (-> item :data :subreddit)}
     :subj {:type (get work-types (:kind item) "unknown")
            :title (-> item :data :title)
            :issued occurred-at-iso8601}}))

; API
(defn parse-page
  "Parse response JSON to a page of Actions."
  [url json-data]
  (let [parsed (json/read-str json-data :key-fn keyword)]
    {:url url
     :extra {:after (-> parsed :data :after)
             :before (-> parsed :data :before)}
     :actions (map api-item-to-action (-> parsed :data :children))}))

(def auth-sleep-duration
  "Back off for a bit if we face an auth problem"
  ; 5 mins
  (* 1000 60 5))

(defn fetch-page
  "Fetch the API result, return a page of Actions."
  [evidence-record-id domain after-token]

  (when (nil? @reddit-token)
    (log/info "Got no token, trying to get one.")
    (renew-reddit-token evidence-record-id))

  (let [url (str "https://oauth.reddit.com/domain/" domain "/new.json?sort=new&after=" after-token)]

    ; If the API returns an error
    (try

      (evidence-log/log!
       {:i "a0035"
        :s agent-name
        :c "reddit-api"
        :f "response"
        :u url
        :r evidence-record-id})

      (try-try-again
       {:sleep 30000 :tries 10}
       #(let [result (client/get url {:headers {"User-Agent" user-agent
                                                "Authorization" (str "bearer " @reddit-token)}
                                      :throw-exceptions false})]
          (log/info "Fetched" url)

          (evidence-log/log!
           {:i "a000f"
            :s agent-name
            :c "reddit-api"
            :f "response"
            :u url
            :r evidence-record-id
            :e (:status result)})

          (condp = (:status result)
            200 (parse-page url (:body result))
            404 {:url url :actions [] :extra {:after nil :before nil :error "Result not found"}}
            429 (do (log/info "Too many requests")
                    (Thread/sleep auth-sleep-duration))
            401 (do
                  (log/error "Unauthorized to access" url)
                  (log/error "Body of error response:" (:body url))
                  (log/info "Taking a nap...")
                  (renew-reddit-token evidence-record-id)
                  (Thread/sleep auth-sleep-duration)
                  (log/info "Woken up!")
                  (throw (new Exception "Unauthorized")))
            (do
              (log/error "Unexpected status code" (:status result) "from" url)
              (log/error "Body of error response:" (:body url))
              (throw (new Exception "Unexpected status"))))))

      (catch Exception ex (do

                            (evidence-log/log!
                             {:i "a0010"
                              :s agent-name
                              :c "reddit-api"
                              :f "error"
                              :u url
                              :r evidence-record-id})

                            (log/error "Error fetching" url)
                            (log/error "Exception:" ex)
                            {:url url :actions [] :extra {:after nil :before nil :error "Failed to retrieve page"}})))))

; We still want to throttle access to the Reddit API.
(def fetch-page-throttled (throttle-fn fetch-page 20 :minute))

(defn fetch-pages
  "Lazy sequence of pages for the domain."
  ([evidence-record-id domain]
   (fetch-pages evidence-record-id domain nil))

  ([evidence-record-id domain after-token]
   (let [result (fetch-page-throttled evidence-record-id domain after-token)

          ; Token for next page. If this is null then we've reached the end of the iteration.
         after-token (-> result :extra :after)]

     (if after-token
       (lazy-seq (cons result (fetch-pages evidence-record-id domain after-token)))
       [result]))))

(defn all-action-dates-after?
  [date page]
  (let [dates (map #(-> % :occurred-at coerce/from-string) (:actions page))]
    (every? #(clj-time/after? % date) dates)))

(defn fetch-parsed-pages-after
  "Return a sequence of pages of Actions that occured after the given time."
  [evidence-record-id domain date]
  (let [pages (fetch-pages evidence-record-id domain)]
    (take-while (partial all-action-dates-after? date) pages)))

(defn check-domain
  [domain artifact-map cutoff-date]
  (let [base-record (util/build-evidence-record manifest artifact-map)
        evidence-record-id (:id base-record)]

    (log/info "Evidence record:" evidence-record-id
              "Domain:" domain
              "Cutoff date:" (str cutoff-date))

    (let [pages (fetch-parsed-pages-after evidence-record-id domain cutoff-date)
          evidence-record (assoc base-record
                                 :extra {:cutoff-date (str cutoff-date) :queried-domain domain}
                                 :pages pages)]

      (log/info "Sending package...")
      (util/send-evidence-record manifest evidence-record))))

(defn main
  "Check all domains for unseen links."
  []
  (log/info "Start crawl all Domains on Reddit at" (str (clj-time/now)))

  (evidence-log/log! {:i "a0011" :s agent-name :c "scan" :f "start"}) (let [artifact-map (util/fetch-artifact-map manifest ["domain-list"])
                                                                            [domain-list-url domain-list] (artifact-map "domain-list")
                                                                            domains (clojure.string/split domain-list #"\n")

        ; Take 5 hours worth of pages to make sure we cover everything. The Percolator will dedupe.
                                                                            num-domains (count domains)

                                                                            results (pmap (fn [domain]
                                                                                            (checkpoint/run-checkpointed!
                                                                                             ["reddit" "domain-query" domain]
                                                                                             (clj-time/hours 1) ; Do each search at most every hour.
                                                                                             (clj-time/years 5) ; Don't page back past 5 years.
                                                                                             #(check-domain domain artifact-map %)))
                                                                                          domains)]

                                                                        (dorun results))

  (evidence-log/log! {:i "a0012" :s agent-name :c "scan" :f "finish"})

  (log/info "Finished scan."))

(def manifest
  {:agent-name agent-name
   :source-id source-id
   :license util/cc-0
   :source-token source-token
   :schedule [[main (clj-time/hours 1)]]
   :runners []})
