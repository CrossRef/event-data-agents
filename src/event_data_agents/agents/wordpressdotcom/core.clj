(ns event-data-agents.agents.wordpressdotcom.core
  "Search the Wordpress.com API for every domain.

   Schedule and checkpointing:
   Continually loop over the domain list.
   Search for each domain no more than once per C."
  (:require [event-data-agents.util :as util]
            [event-data-agents.checkpoint :as checkpoint]
            [clojure.tools.logging :as log]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]
            [event-data-common.evidence-log :as evidence-log]
            [config.core :refer [env]]
            [robert.bruce :refer [try-try-again]]
            [clj-http.client :as client]
            [clojure.data.json :as json])
  (:import [java.util UUID]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.lang3 StringEscapeUtils])
  (:gen-class))

(def source-token "7750d578-d74d-4e92-9348-cd443cbb7afa")
(def agent-name "wordpressdotcom-agent")
(def source-id "wordpressdotcom")
(def user-agent "CrossrefEventDataBot (eventdata@crossref.org)")

(declare manifest)

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(def query-url-base "https://en.search.wordpress.com")

; The Wordpress API is very flaky and can paginate erratically. Never go past this many pages.
(def max-pages 20)

(defn stop-at-dupe
  "Take from items until we meet a duplicate pair.
   Wordpress.com API sometimes sends dulicate pages. Bail out if that happens."
  [items]
  (let [cnt (count (take-while (partial apply not=) (partition-all 2 1 items)))]
    (take (inc cnt) items)))

(defn fetch-pages
  "Retrieve Wordpress results as a lazy seq of parsed pages of {:url :results}."
  ([evidence-record-id domain] (fetch-pages evidence-record-id domain 1))
  ([evidence-record-id domain page-number]
    ; If we're blocked after 20 tries (17 minute delay) then just give up the whole thing.


   (let [query-params {"f" "json"
                       "size" 20
                       "t" "post"
                       "q" (str \" domain \")
                       "page" page-number
                       "s" "date"}

          ; The API returns nil value to represent the end of results, but also sometimes at random during pagination.
          ; Because most queries result in nil, don't re-try every nil. Instead only do this when we're at least into page 2 of a result set.
          ; This is a compromise between retrying every single failed result and potentially missing out on some.
          ; This is just a quirk of the Wordpress.com API.
         url (str query-url-base "?" (client/generate-query-string query-params))
         result-parsed (try
                         (evidence-log/log! {:i "a0033" :s agent-name :c "wordpressdotcom-api" :f "request"
                                             :p page-number :v domain :u url}) (try-try-again {:tries 3 :decay :double :delay 1000}
                                                                                              (fn [] (let [result (client/get query-url-base {:query-params query-params :headers {"User-Agent" user-agent}})
                                                                                                           body (when-let [body (:body result)] (json/read-str body))]
                                                                                                       (if body
                                                                                                         body
                                                                                                         (when (> page-number 1)
                                                                                                           (throw (new Exception)))))))
                         (catch Exception ex nil))]

     (evidence-log/log! {:i "a0034" :s agent-name :c "wordpressdotcom-api" :f "response"
                         :p page-number :v domain :u url :e (if result-parsed "t" "f")})

     (log/info "Retrieved" url)

      ; The API just returns nil when there's no more data (or we got another failure).
     (if (nil? result-parsed)
       nil
       (lazy-cat [{:url url :results result-parsed}]
                 (fetch-pages evidence-record-id domain (inc page-number)))))))

(defn parse-item
  "Parse a page item into an Action."
  [item]
  (let [url (get item "link")
        title (get item "title")
        date (-> item
                 (get "epoch_time")
                 Long/parseLong
                 (* 1000)
                 clj-time-coerce/from-long)

        date-str (clj-time-format/unparse date-format date)

        ; Use the URL of the blog post as the action identifier.
        ; This means that the same blog post in different feeds (or even sources) will have the same ID.
        action-id (DigestUtils/sha1Hex ^String url)]
    {:id action-id
     :url url
     :relation-type-id "discusses"
     :occurred-at date-str
     :observations [{:type :content-url :input-url url :sensitive true}]
     :subj {:type "post-weblog"
      ; We find occasional HTML character entities.
            :title (StringEscapeUtils/unescapeHtml4 title)}}))

(defn parse-page
  [page]
  {:url (:url page) :actions (map parse-item (:results page))})

(defn take-pages-after
  [date pages]
  "Accept a seq of pages of Actions. Take pages until we get a page on which every entry occurs on or before the date."
  (take-while (fn [page]
                (some #(->> % :occurred-at (clj-time-format/parse date-format) (clj-time/before? date)) (:actions page)))
              pages))

(defn fetch-parsed-pages-after
  "Return a seq of Percolator pages from the API that concern the given domain.
   As many pages as have events that occurred after the date."
  [evidence-record-id date domain]
  (take-pages-after
   date
   (map
    parse-page
    (->> domain (fetch-pages evidence-record-id) (take max-pages) stop-at-dupe))))

(defn check-domain
  [domain artifact-map cutoff-date]

  (log/info "Query for domain:" domain
            "Cutoff-date:" (str cutoff-date))

  (let [base-record (util/build-evidence-record manifest artifact-map)
        evidence-record-id (:id base-record)

        pages (doall (fetch-parsed-pages-after evidence-record-id cutoff-date domain))
        evidence-record (assoc base-record
                               :extra {:cutoff-date (clj-time-format/unparse date-format cutoff-date)
                                       :queried-domain domain}
                               :pages pages)]
    (log/info "Sending package...")
    (util/send-evidence-record manifest evidence-record)
    (log/info "Sent package.")))

(defn main
  "Check all domains for unseen links."
  []
  (log/info "Start crawl all Domains on Wordpress.com at" (str (clj-time/now)))

  (evidence-log/log! {:i "a0031" :s agent-name :c "scan-all-sites" :f "start"})

  (let [artifact-map (util/fetch-artifact-map manifest ["domain-list"])
        [domain-list-url domain-list] (artifact-map "domain-list")
        domains (clojure.string/split domain-list #"\n")

        num-domains (count domains)]

    (doseq [domain domains]

      (checkpoint/run-checkpointed!
       ["wordpressdotcom" "domain-scan" domain]
       (clj-time/days 1) ; Scan at most once per day per domain.
       (clj-time/years 10) ; Scan back no further than 10 years.
       #(check-domain domain artifact-map %))))

  (evidence-log/log! {:i "a0032" :s agent-name :c "scan-all-sites" :f "finish"})

  (log/info "Finished scan."))

(def manifest
  {:agent-name agent-name
   :source-id source-id
   :source-token source-token
   :license util/cc-0
   :schedule [[main (clj-time/hours 1)]]
   :runners []})
