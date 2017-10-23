(ns event-data-agents.agents.reddit-links.core
  "Take URL of every entry on a list of subreddits.

  Schedule and checkpointing:
  Continually loop over the subreddit list.
  Check each subreddit no more than once per C."
  (:require [event-data-agents.util :as util]
            [event-data-agents.checkpoint :as checkpoint]
            [event-data-agents.agents.reddit.core]
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
  (:import [java.util UUID]
           [java.net URL]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.lang3 StringEscapeUtils])
  (:gen-class))

(def source-token "93df90e8-1881-40fc-b19d-49d78cc9ee24")
(def user-agent
  "HTTP User Agent. A bit more Reddit-specific detail added."
  "CrossrefEventDataBot (eventdata@crossref.org) (by /u/crossref-bot labs@crossref.org)")

(def agent-name "reddit-links-agent")
(def source-id "reddit-links")
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
      {:i "a0013"
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

(def interested-kinds
  #{"t3"}) ; Link

(def uninterested-hostnames
  "Ignore links on these domains because they're conversations on reddit. We're looking for external links."
  #{"reddit.com" "www.reddit.com"})

(defn api-item-to-action
  [domains item]
  (let [occurred-at-iso8601 (clj-time-format/unparse date-format (coerce/from-long (* 1000 (long (-> item :data :created_utc)))))
        link (-> item :data :url)
        unescaped-link (StringEscapeUtils/unescapeHtml4 link)
        cleaned-link (url-cleanup/remove-tracking-params unescaped-link)
        host (try (.getHost (new URL unescaped-link)) (catch Exception e nil))]

    ; We only care about things that are links and that are links to external sites.
    ; Reddit discussions are a seprate thing.
    (when (and (interested-kinds (:kind item))
               (not (uninterested-hostnames host))
               (not (domains host)))

    {:id (DigestUtils/sha1Hex ^String cleaned-link)
     :url cleaned-link
     :relation-type-id "discusses"
     :occurred-at occurred-at-iso8601
     :subj {}
     :observations [{:type :content-url :input-url cleaned-link :sensitive true}]})))

; API
(defn parse-page
  "Parse response JSON to a page of Actions."
  [domains url json-data]
  (let [parsed (json/read-str json-data :key-fn keyword)]
    {:url url
     :extra {
      :after (-> parsed :data :after)
      :before (-> parsed :data :before)}
     ; parse returns nil for links we don't want. Don't include null actions.
     :actions (keep (partial api-item-to-action domains) (-> parsed :data :children))}))

(def auth-sleep-duration
  "Back off for a bit if we face an auth problem"
  ; 5 mins
  (* 1000 60 5))

(defn fetch-page
  "Fetch the API result, return a page of Actions."
  [evidence-record-id domains subreddit after-token]

  (when (nil? @reddit-token)
    (log/info "Got no token, trying to get one.")
    (renew-reddit-token evidence-record-id))

  (let [url (str "https://oauth.reddit.com" subreddit "/new.json?sort=new&after=" after-token)]
    (log/info "Fetch" url)

    (try

      (evidence-log/log!
            {:i "a0036"
             :s agent-name
             :c "reddit-api"
             :f "request"
             :u url
             :r evidence-record-id})

      (try-try-again
        {:sleep 30000 :tries 10}
        #(let [result (client/get url {:timeout 60000
                                       :headers {"User-Agent" user-agent
                                                 ; Optimistically use token. If it doesn't work, retry in error handler.
                                                 "Authorization" (str "bearer " @reddit-token)}
                                       :throw-exceptions false})]
          
          (log/info "Fetched" url)

          (evidence-log/log!
            {:i "a0014"
             :s agent-name
             :c "reddit-api"
             :f "response"
             :u url
             :r evidence-record-id
             :e (:status result)})

          (condp = (:status result)
            200 (parse-page domains url (:body result))
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
          {:i "a0015"
            :s agent-name
           :c "reddit-api"
           :f "error"
           :u url
           :r evidence-record-id})

        (log/error "Error fetching" url)
        (log/error "Exception:" ex)
        {:url url :actions [] :extra {:after nil :before nil :error "Failed to retrieve page"}})))))

(def fetch-page-throttled (throttle-fn fetch-page 20 :minute))

(defn fetch-pages
  "Lazy sequence of pages for the subreddit."
  ([evidence-record-id domains subreddit]
    (fetch-pages evidence-record-id domains subreddit nil))

  ([evidence-record-id domains subreddit after-token]
    (let [result (fetch-page-throttled evidence-record-id domains subreddit after-token)
          ; Token for next page. If this is null then we've reached the end of the iteration.
          after-token (-> result :extra :after)]

      (if after-token
        (lazy-seq (cons result (fetch-pages evidence-record-id domains subreddit after-token)))
        [result]))))

(defn all-action-dates-after?
  [date page]
  (let [dates (map #(-> % :occurred-at coerce/from-string) (:actions page))]
    (every? #(clj-time/after? % date) dates)))

(defn fetch-parsed-pages-after
  "Fetch seq parsed pages of Actions until all actions on the page happened before the given time."
  [evidence-record-id domains subreddit date]
  (let [all-pages (fetch-pages evidence-record-id domains subreddit)
        pages (take-while (partial all-action-dates-after? date) all-pages)]
    pages))

(defn check-subreddit
  [subreddit artifact-map domain-set cutoff-date]
   (let [base-record (util/build-evidence-record manifest artifact-map)
        evidence-record-id (:id base-record)]

    (log/info
      "Evidence record:" evidence-record-id
      "Query subreddit:" subreddit
      "Cutoff date:" (str cutoff-date))

    ; Need to realize the lazy sequence as we're stuffing it into an Evidence Record.
    (let [pages (doall (fetch-parsed-pages-after evidence-record-id domain-set subreddit cutoff-date))
          evidence-record (assoc base-record
                            :extra {:cutoff-date (str cutoff-date) :queried-subreddit subreddit}
                            :pages pages)]
      (log/info "Sending evidence record...")
      (util/send-evidence-record manifest evidence-record))))


(defn main
  "Check all subreddits for unseen links."
  []
  (log/info "Start crawl all Domains on Reddit at" (str (clj-time/now)))

  (evidence-log/log! {
    :i "a0016" :s agent-name :c "scan" :f "start"})

  (let [artifact-map (util/fetch-artifact-map manifest ["domain-list" "subreddit-list"])
        [domain-list-url domain-list] (artifact-map "domain-list")
        domains (clojure.string/split domain-list #"\n")
        domain-set (set domains)

        [subreddit-list-url subreddit-list] (artifact-map "subreddit-list")
        subreddits (set (clojure.string/split subreddit-list #"\n"))

        results (pmap (fn [subreddit]
                        (checkpoint/run-checkpointed!
                          ["reddit-links" "subreddit-query" subreddit]
                          (clj-time/hours 1) ; Scan each subreddit at most once an hour.
                          (clj-time/years 5) ; Don't page back past 5 years.
                          #(check-subreddit subreddit artifact-map domain-set %)))
                      subreddits)]

    (dorun results))

    (evidence-log/log! {
      :i "a0017" :s agent-name :c "scan" :f "finish"})
    (log/info "Finished scan."))

(def manifest
  {:agent-name agent-name
   :source-id source-id
   :license util/cc-0
   :source-token source-token
   :schedule [[main (clj-time/hours 1)]]
   :runners []})
