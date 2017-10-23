(ns event-data-agents.agents.newsfeed.core
  "Process every post that appears in a news feed.

   Schedule and checkpointing:
   Continually loop over the newsfeed list.
   Check each newsfeed no more than once per C."

  (:require [event-data-agents.util :as util]
            [event-data-agents.checkpoint :as checkpoint]
            [event-data-common.evidence-log :as evidence-log]
            [event-data-common.url-cleanup :as url-cleanup]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [config.core :refer [env]]
            [clj-time.core :as clj-time]
            [clj-time.coerce :as clj-time-coerce]
            [clj-time.format :as clj-time-format]
            [throttler.core :refer [throttle-fn]])
  (:import [java.net URL]
           [java.io InputStreamReader]
           [com.rometools.rome.feed.synd SyndFeed SyndEntry SyndContent]
           [com.rometools.rome.io SyndFeedInput XmlReader]
           [org.apache.commons.codec.digest DigestUtils]
           [org.apache.commons.lang3 StringEscapeUtils])
   (:gen-class))

(def agent-name "newsfeed-agent")
(def source-id "newsfeed")
(def source-token "c1bfb47c-39b8-4224-bb18-96edf85e3f7b")
(declare manifest)

(defn choose-best-link
  "From a seq of links for the same resource, choose the best one based on newsfeed heuristics."
  [& urls]
  (->> urls
      ; Remove those that aren't URLs.
      (keep #(try (new URL %) (catch Exception _ nil)))
      (remove nil?)
      ; Rank by desirability. Lowest is best.
      (sort-by #(cond
                  ; feeds.feedburner.com's URLs go via a Google proxy. Ignore those if possible.
                  (= (.getHost %) "feedproxy.google.com") 5
                  :default 1))
      first
      str))

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(defn parse-section
  "Parse a SyndEntry into an Action. Discard the summary and use the url type only.
  The Percolator will follow the link to get the content."
  [feed-url fetch-date-str ^SyndEntry entry]
  (let [title (.getTitle entry)
        
        ; Only 'link' is specified as being the URL, but feedburner includes the real URL only in the ID.
        ; Remove tracking parameters immediately so the action-id is as effective as possible at deduping.
        url (url-cleanup/remove-tracking-params
              (choose-best-link (.getLink entry) (.getUri entry)))
        
        ; Updated date is the date the blog is reported to have been published via the feed.
        ; Failing that, now, the time we ingested this post URL.
        updated (try
                   (clj-time-coerce/from-date (or (.getUpdatedDate entry)
                                                  (.getPublishedDate entry)))
                   (catch Exception e (clj-time/now)))

        ; Use the URL of the blog post as the action identifier.
        ; This means that the same blog post in different feeds (or even sources) will have the same ID.
        action-id (DigestUtils/sha1Hex ^String url)]
    
    {:id action-id
     :url url
     :relation-type-id "discusses"
     :occurred-at (clj-time-format/unparse date-format updated)
     :observations [{:type :content-url :input-url url :sensitive true}]
     :extra {:feed-url feed-url}
     :subj {
      ; Title appears as CDATA containing an HTML encoded string (different to XML encoded!) 
      :title (StringEscapeUtils/unescapeHtml4 title)}}))

(defn actions-from-xml-reader
  [evidence-record-id url ^XmlReader reader]
  (let [input (new SyndFeedInput)
        feed (.build input reader)
        entries (.getEntries feed)
        parsed-entries (map
                         (partial
                          parse-section
                          url
                          (clj-time-format/unparse date-format (clj-time/now)))
                         entries)]
  
    (evidence-log/log! {
      :i "a0008" :s agent-name :c "newsfeed" :f "parsed-entries"
      :v (count parsed-entries) :r evidence-record-id :u url})

  parsed-entries))

(defn evidence-record-for-feed
  "Get list of parsed Actions from the feed url. Return as an Evidence Record."
  [manifest artifact-map feed-url]
  (let [base-record (util/build-evidence-record manifest artifact-map)
        evidence-record-id (:id base-record)]

    (log/info "Retrieve latest from feed:" feed-url)

    (evidence-log/log!
      {:i "a0009" :s agent-name :c "remote-newsfeed" :f "request" :u feed-url :r evidence-record-id})

    (try
      (let [reader (new XmlReader (new URL feed-url))
            actions (actions-from-xml-reader evidence-record-id feed-url reader)]
        
        ; Parse succeeded.
        (evidence-log/log!
          {:i "a000a"
           :s agent-name
           :c "remote-newsfeed"
           :f "parse"
           :e "t"
           :u feed-url
           :r evidence-record-id})

        (assoc base-record
          ; One page of actions.
          :pages [{:actions actions}]))

      ; e.g. com.rometools.rome.io.ParsingFeedException
      (catch Exception ex
        (do
          ; Parse failed. Same log ID.
          (evidence-log/log!
            {:i "a000a"
             :s agent-name
             :c "remote-newsfeed"
             :f "parse"
             :e "f"
             :u feed-url
             :r evidence-record-id})

          (log/info "Error parsing data from feed url:" feed-url)
          (.printStackTrace ex))))))

(def check-every
  "Visit every newsfeed at most this often."
  (clj-time/hours 2))

(defn check-newsfeed
  [artifact-map newsfeed-url]
  (log/info "Check newsfeed url" newsfeed-url)
  (let [evidence-record (evidence-record-for-feed manifest artifact-map newsfeed-url)]
    (util/send-evidence-record manifest evidence-record)))

(defn main
  "Main function for Newsfeed Agent."
  []

  (evidence-log/log! {
    :i "a000c" :s agent-name :c "scan" :f "start"})

  (log/info "Start crawl all newsfeeds at" (str (clj-time/now)))
  (let [artifact-map (util/fetch-artifact-map manifest ["newsfeed-list"])
        newsfeed-list-content (-> artifact-map (get "newsfeed-list") second)
        newsfeed-set (clojure.string/split newsfeed-list-content #"\n")

        ; Check every newsfeed, checkpointing per newsfeed.
        results (pmap
                  #(checkpoint/run-checkpointed!
                    ["newsfeed" "feed-check" %]
                    (clj-time/hours 1)
                    ; We discard the last-run date anyway, as newsfeeds
                    ; only ever give one page of results.
                    (clj-time/hours 1)
                    (fn [_] (check-newsfeed artifact-map %)))
                  newsfeed-set)]

    (log/info "Got newsfeed-list artifact:" (-> artifact-map (get "newsfeed-list") first))
    
    (dorun results)
    
    (evidence-log/log! {:i "a000d" :s agent-name :c "scan" :f "finish"})
    
    (log/info "Finished scan.")))

(def manifest
  {:agent-name agent-name
   :source-id source-id
   :license util/cc-0
   :source-token source-token
   ; Pause 30 minutes between scans to ensure we don't get stuck in a tight loop.
   ; Each newsfeed is individually checkpointed, so no danger in this being too low.
   :schedule [[main (clj-time/minutes 30)]]})

