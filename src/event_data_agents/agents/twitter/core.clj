(ns event-data-agents.agents.twitter.core
  (:require [event-data-agents.util :as util]
            [event-data-common.evidence-log :as evidence-log]
            [clojure.core.async :refer [go-loop thread buffer chan <!! >!! >! <! timeout alts!!]]
            [config.core :refer [env]]
            [clj-time.coerce :as coerce]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [event-data-common.backoff :as backoff]
            [clj-http.client :as client]
            [throttler.core :refer [throttle-fn]]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:import [java.util UUID]
           [java.io File]
           [org.apache.commons.codec.digest DigestUtils]
           [java.util.zip GZIPInputStream])
  (:gen-class))

(def source-token "45a1ef76-4f43-4cdc-9ba8-5a6ad01cc231")
(def agent-name "twitter-agent")
(def source-id "twitter")
(declare manifest)

(def unknown-url "http://id.eventdata.crossref.org/unknown")

(def date-format
  (:date-time-no-ms clj-time-format/formatters))

(def input-date-format
  (:date-time clj-time-format/formatters))

(defn tweet-url->tweet-id
  [url]
  (re-find #"[\d]+$" url))

(defn tweet-url->uri
  "Convert a Twitter-supplied URL to a URI that's approved for redistribution."
  [url]
  (some->> url tweet-url->tweet-id (str "twitter://status?id=")))


(defn actor-url->username
  [url]
  (some->> url (re-find #"/([^/]+)$") second))

(defn actor-url->uri
  "Convert a Twitter-supplied actor URL to a URI that's approved for redistribution."
  [url]
  (some->> url actor-url->username (str "twitter://user?screen_name=")))

; http://support.gnip.com/apis/powertrack2.0/rules.html
(def max-rule-length 2048)

; Rules 

(defn- format-gnip-ruleset
  "Format a set of string rules into a JSON object."
  [rule-seq]
  (let [id-base (str (clj-time/now))
        structure {"rules" (map #(hash-map "value" %) rule-seq)}]
    (json/write-str structure)))

(defn- parse-gnip-ruleset
  "Parse the Gnip ruleset into a seq of rule string."
  [json-string]
  (let [structure (json/read-str json-string :key-fn keyword)]
    (map :id (:rules structure))))

(defn- fetch-rule-ids-from-gnip
  "Fetch the current rule ID set from Gnip."
  []
  (let [fetched (client/get (:twitter-gnip-rules-url env) {:basic-auth [(:twitter-gnip-username env) (:twitter-gnip-password env)]})
        rules (-> fetched :body parse-gnip-ruleset)]
    (set rules)))

(defn- create-rule-from-domain
  "Create a Gnip rule from a full domain, e.g. www.xyz.com, if valid or nil."
  [full-domain]
  ; Basic sense check.
  (when (> (.length full-domain) 3)
    (str "url_contains:\"//" full-domain "/\"")))

(defn- create-rule-from-prefix
  "Create a Gnip rule from a DOI prefix, e.g. 10.5555"
  [prefix]
  (str "contains:\"" prefix "/\""))

(defn- add-rules
  "Add rules to Gnip."
  [rules]
  (let [result (client/post (:twitter-gnip-rules-url env) {:body (format-gnip-ruleset rules) :basic-auth [(:twitter-gnip-username env) (:twitter-gnip-password env)]})]
    (when-not (#{200 201} (:status result))
      (log/fatal "Failed to add rules" result))))

(defn- remove-rule-ids
  "Add rules to Gnip."
  [rule-ids]
  (let [result (client/post (:twitter-gnip-rules-url env) {:body (json/write-str {"rule_ids" rule-ids}) :query-params {"_method" "delete"} :basic-auth [(:twitter-gnip-username env) (:twitter-gnip-password env)]})]
    (when-not (#{200 201} (:status result))
      (log/fatal "Failed to delete rules" result))))

(defn compact-rules
  [rules]
  "Take a seq of rules, return a tuple of combined rule (up to length) and rest."
  (loop [[rule & rules] (rest rules)
         acc (first rules)
         compacted (list)]
    (if-not rule
      compacted
      (let [new-rule (str acc " OR " rule)]
        (if (> (.length new-rule) max-rule-length)
          ; If appending this one makes it too long, accumulate the compacted ones, and use it as the starter for the next compacted rule.
          (recur rules rule (conj compacted acc))
          (recur rules new-rule compacted))))))

(defn main-update-rules
  "Perform complete update cycle of Gnip rules.
  Do this by fetching the list of domains and prefixes from the domain list Artifact,
  creating a rule-set then diffing with what's already in Gnip."
  []

  (evidence-log/log! {:i "a0037" :s agent-name :c "update-rules" :f "start"})

  (let [old-rule-ids (fetch-rule-ids-from-gnip)

        artifact-map (util/fetch-artifact-map manifest ["domain-list" "doi-prefix-list"])
        [_ domain-list] (artifact-map "domain-list")
        domains (clojure.string/split domain-list #"\n")

        [_ doi-prefix-list] (artifact-map "doi-prefix-list")
        doi-prefix-list (set (clojure.string/split domain-list #"\n"))

        doi-prefix-rules (map create-rule-from-prefix doi-prefix-list)
        domain-rules (map create-rule-from-domain domains)
        rules (set (concat doi-prefix-rules domain-rules))
        compacted-rules (compact-rules rules)]

    (log/info "Old rules " (count old-rule-ids) ", up to date rules " (count rules))

    (evidence-log/log! {:i "a0038" :s agent-name :c "update-rules" :f "domain-count" :v (count domains)})

    (evidence-log/log! {:i "a0039" :s agent-name :c "update-rules" :f "prefix-count" :v (count doi-prefix-list)})

    (evidence-log/log! {:i "a003a" :s agent-name :c "update-rules" :f "rule-count" :v (count rules)})

    (log/info "Artifact provided" (count domains) "domains")
    (log/info "Artifact provided" (count doi-prefix-list) "DOI prefixes")

    (log/info "Resulting in " (count rules) "domain rules")
    (log/info "Artifact provided" (count doi-prefix-rules) "DOI prefixe rules")
    (log/info "Total rules compacted:" (count compacted-rules))

    (log/info "Add" (count compacted-rules) "new rules")
    (add-rules compacted-rules)

    (evidence-log/log! {:i "a003b" :s agent-name :c "update-rules" :f "send-new-rules" :v (count compacted-rules)})

    (evidence-log/log! {:i "a003c" :s agent-name :c "update-rules" :f "remove-old-rules" :v (count old-rule-ids)})

    (log/info "Remove " (count old-rule-ids) "old rules")
    (log/info "Old Rule IDs: " old-rule-ids)
    (doseq [chunk (partition-all 1000 old-rule-ids)]
      (log/info "Done chunk of " (count chunk))
      (remove-rule-ids chunk)))

  (evidence-log/log! {:i "a003d" :s agent-name :c "update-rules" :f "finished"}))

(defn entry->action
  "Parse a tweet input (keyword-keyed parsed JSON String) into an Action.
   On input error log and return nil."
  [parsed]
    (cond
      (:error parsed)
      (do
        (log/error "Gnip error:" (-> parsed :error :message)
        nil))

      (:info parsed)
      (do
        (log/info "Gnip info:" (:info parsed)))

      :default
      (let [; comes in with milliseconds but CED schema prefers non-millisecond version.
            posted-time-str (:postedTime parsed)
            posted-time (clj-time-format/parse input-date-format posted-time-str)

            ; URL as posted (removing nils).
            expanded-urls (->> parsed :gnip :urls (keep :expanded_url))

            ; URLs as expanded by Gnip (removing nils).
            original-urls (->> parsed :gnip :urls (keep :url))

            urls (set (concat expanded-urls original-urls))

            url (:link parsed unknown-url)
            uri (tweet-url->uri url)

            matching-rules (->> parsed :gnip :matching_rules (keep :id))

            plaintext-observations [{:type "plaintext"
                                     :input-content (:body parsed)
                                     :sensitive true}]
            url-observations (map (fn [url]
                                    {:type "url"
                                     :sensitive false
                                     :input-url url}) urls)

            internal-id (tweet-url->tweet-id url)
            title (str "Tweet " internal-id)]

        ; Use the URL from Twitter for the Action ID (deduplication)
        {:id (DigestUtils/sha1Hex ^String url)
         :url uri
         :occurred-at (clj-time-format/unparse date-format posted-time)
         :extra {:gnip-matching-rules matching-rules}
         :subj {:title title
               ; preserve original time string
                :issued posted-time-str
                :author {:url (-> parsed :actor :link actor-url->uri)}
                :original-tweet-url (-> parsed :object :link tweet-url->uri)
                :original-tweet-author (-> parsed :object :actor :link actor-url->uri)
                :alternative-id internal-id}
         :relation-type-id "discusses"
         :observations (doall (concat plaintext-observations
                                      url-observations))})))

(def timeout-duration
  "Time to wait for a new line before timing out. This should be greater than the rate we expect to get tweets. 
   Two minutes should cover the worst case."
  120000)

(defn run
  [channel url]
  "Send parsed events to the chan and block.
   On exception, log and exit (allowing it to be restarted)"

  (evidence-log/log! {:i "a0021" :s agent-name :c "twitter-api" :f "connect"})

  (try
    (let [response (client/get
                    url
                    {:as :stream :basic-auth [(:twitter-gnip-username env) (:twitter-gnip-password env)]})
          stream (:body response)
          lines (line-seq (io/reader stream))]
      (loop [lines lines]
        (when lines
          (let [timeout-ch (timeout timeout-duration)
                result-ch (thread (try [(or (first lines) :nil) (rest lines)] (catch java.io.IOException ex (do (log/error "Error getting line from PowerTrack:" (.getMessage ex)) nil))))
                [[x xs] chosen-ch] (alts!! [timeout-ch result-ch])]

              ; timeout: x is nil, xs is nil
              ; null from server: x is :nil, xs is rest
              ; data from serer: x is data, xs is rest
            (cond
                ; nil from timeout
              (nil? x) (.close stream)

                ; empty string from API, ignore
              (clojure.string/blank? x) (recur xs)

                ; :nil, deliberately returned above
              (= :nil x) (recur xs)
              :default (let [parsed (entry->action (json/read-str x :key-fn keyword))]
                         (when parsed
                           (>!! channel parsed))
                         (recur xs)))))))
    (catch Exception ex (do

                          (evidence-log/log! {:i "a0022" :s agent-name :c "twitter-api" :f "disconnect"})

                          (log/info (.getMessage ex))))))

(defn run-loop
  [channel url]
  (loop [timeout-delay 30000]
    (log/info "Starting / restarting.")
    (run channel url)
    (log/info "Stopped")
    (log/info "Try again in" timeout-delay "ms")
    (Thread/sleep timeout-delay)
    (recur timeout-delay)))

; Nice big buffer, as they're required for transducers.
(def action-input-buffer 1000000)

; Bunch requests up into chunks of this size.
(def action-chunk-size 20)

; A chan that partitions inputs into large chunks.
(def action-chan (delay (chan action-input-buffer (partition-all action-chunk-size))))

(defn main-ingest-stream
  "Run the stream ingestion.
   Blocks forever."
  []

  (evidence-log/log! {:i "a0023" :s agent-name :c "ingest" :f "start"})

  ; Both args ignored.
  (let [url (:twitter-powertrack-endpoint env)]
    (log/info "Connect to" url)
    (run-loop @action-chan url)))

(defn main-send
  "Take chunks of Actions from the action-chan, assemble into Percolator Evidence Record, put them on the input-package-channel.
   Blocks forever."
  []

  (evidence-log/log! {:i "a0024" :s agent-name :c "process" :f "start"})

  ; Take chunks of inputs, a few tweets per input bundle.
  ; Gather then into a Page of actions.
  (log/info "Waiting for chunks of actions...")
  (let [channel @action-chan
        ; No artifacts consumed here.
        artifact-map {}]
    (loop [actions (<!! channel)]
      (log/info "Got a chunk of" (count actions) "actions")

      (evidence-log/log! {:i "a0025" :s agent-name :c "process" :f "got-chunk" :v (count actions)})

      (let [evidence-record (assoc
                             (util/build-evidence-record manifest artifact-map)
                             :pages [{:actions actions}])]

        (util/send-evidence-record manifest evidence-record)

        (log/info "Sent a chunk of" (count actions) "actions"))
      (recur (<!! channel)))))

(defn main-scan-directory
  "Scan a directory of gzipped Gnip messages. This is the format returned by one-off manual requests to GNIP.
   Each file is a Gzipped sequence of JSON lines."
  [dir-path]
  (let [files (filter #(.isFile %) (file-seq (io/file dir-path)))]
    ; Each file can result in quite a lot of data, so address them one by one.
    ; Alternative is trying to create a lazy seq of actions over files, which means buffering each file's
    ; worth of data before closing the file handle.
    (doseq [file (drop 1 files)]
      (log/info "Read" file)
      (try
        (with-open [reader (io/reader (GZIPInputStream. (io/input-stream file)))]
          (let [lines (line-seq reader)
                parsed-lines (map #(json/read-str % :key-fn keyword) lines)
                ; Can return nil if the message didn't contain data, e.g. an :info message. 
                ; Filter these out.
                actions (keep entry->action parsed-lines)
                chunks (partition-all action-chunk-size actions)]

            (doseq [action-chunk chunks]
              (let [evidence-record (assoc
                                     (util/build-evidence-record manifest {})
                                     :pages [{:actions action-chunk}])]
                (util/send-evidence-record manifest evidence-record)
                (log/info "Send" (:id evidence-record))))))

      (catch Exception ex 
        (do (log/error "Can't read" file "as GZIPped JSON" (.printStackTrace ex))))))))




(def manifest
  {:agent-name agent-name
   :source-id source-id
   :source-token source-token
   :license util/cc-0
   :schedule [[main-update-rules (clj-time/days 20)]]
   :daemons [main-ingest-stream main-send]})
