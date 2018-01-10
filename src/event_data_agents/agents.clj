(ns event-data-agents.agents
  (:require [event-data-agents.agents.hypothesis.core :as hypothesis]
            [event-data-agents.agents.newsfeed.core :as newsfeed]
            [event-data-agents.agents.reddit.core :as reddit]
            [event-data-agents.agents.reddit-links.core :as reddit-links]
            [event-data-agents.agents.stackexchange.core :as stackexchange]
            [event-data-agents.agents.twitter.core :as twitter]
            [event-data-agents.agents.wikipedia.core :as wikipedia]
            [event-data-agents.agents.wordpressdotcom.core :as wordpressdotcom]
            [event-data-agents.agents.carberry.core :as carberry])
  (:require [overtone.at-at :as at-at]
            [clj-time.core :as clj-time]
            [clojure.tools.logging :as log]))

; An Agent should present its manifest as a hashmap with the following keys:
; :agent-name - The name of the Agent, for identification purposes.
; :source-id - The source ID for Events. Used to identify where the data came from, also for JWT auth.
; :license - A license URL.
; :source-token - The registered source token that identifies this Agent.
; :schedule - Seq of tuples of [function seconds-delay-between-runs]
; :runners - Seq of functions that should be run and kept running.

(def manifests
  {:hypothesis hypothesis/manifest
   :newsfeed newsfeed/manifest
   :reddit reddit/manifest
   :reddit-links reddit-links/manifest
   :stackexchange stackexchange/manifest
   :twitter twitter/manifest
   :wikipedia wikipedia/manifest
   :wordpressdotcom wordpressdotcom/manifest
   :carberry carberry/manifest})

(defn manifests-for-names
  "Retrieve the manifests for the Agent names.
   Log error if not found but don't error."
   [agent-names]
   (doall
    (keep
      (fn [agent-name]
        (let [manifest (get manifests (keyword agent-name))]
          (when-not manifest
            (log/error "Didn't recognise agent name" agent-name))
          manifest))
      agent-names)))

(defn start-schedule
  "Run the schedule for the given string Agent names.
   at-at starts a daemon thread which will block exit."
  [schedule-pool agent-names]
  (log/info "Start scheduler...")
    
    (doseq [manifest (manifests-for-names agent-names)]
      (log/info "Adding schedule for" (:agent-name manifest))
      (doseq [[function delay-period] (:schedule manifest)]
        (log/info "Add schedule" function "with delay" (str delay-period))
        (at-at/interspaced
          (clj-time/in-millis delay-period)
          function schedule-pool)))

  (log/info "Finished setting up scheduler."))

(defn run-schedule-once
  "Run the job that would have been scheduled for the given Agent names now, then exit."
  [agent-names]
  (log/info "Running one-off Agent schedule jobs now...")
  
  (doseq [manifest (manifests-for-names agent-names)]
    (log/info "Running jobs for" (:agent-name manifest))
    
    (doseq [[function delay-period] (:schedule manifest)]
      (log/info "Running job for" (:agent-name manifest) "...")
      (function))

    (log/info "Done all jobs for" (:agent-name manifest) "."))
  (log/info "Run all one-off Agent jobs now."))

(defn run-daemons
  "For Agents that use a runner, run their run function."
  [agent-names]
  (log/info "Running one-off Agent daemon jobs now...")
  (doseq [manifest (manifests-for-names agent-names)]
    (log/info "Running daemons for" (:agent-name manifest))
    (doseq [f (:daemons manifest)]
      (log/info "Running daemons for" (:agent-name manifest) "...")
      (.start (Thread. f)))
    (log/info "Done all daemons for" (:agent-name manifest) "."))
  (log/warn "Finished running daemons."))

