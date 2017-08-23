(ns event-data-agents.agents
  (:require [event-data-agents.agents.hypothesis.core :as hypothesis]
            [event-data-agents.agents.newsfeed.core :as newsfeed]
            [event-data-agents.agents.reddit.core :as reddit]
            [event-data-agents.agents.reddit-links.core :as reddit-links]
            [event-data-agents.agents.stackexchange.core :as stackexchange]
            [event-data-agents.agents.twitter.core :as twitter]
            [event-data-agents.agents.wikipedia.core :as wikipedia]
            [event-data-agents.agents.wordpressdotcom.core :as wordpressdotcom])
  (:require [clojurewerkz.quartzite.triggers :as qt]
            [clojure.tools.logging :as log]
            [clojurewerkz.quartzite.jobs :as qj]
            [clojurewerkz.quartzite.schedule.daily-interval :as daily]
            [clojurewerkz.quartzite.schedule.calendar-interval :as cal]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.schedule.cron :as qc]))

; An Agent should present its manifest as a hashmap with the following keys:
; :agent-name - The name of the Agent, for identification purposes.
; :source-id - The source ID for Events. Used to identify where the data came from, also for JWT auth.
; :license - A license URL.
; :source-token - The registered source token that identifies this Agent.
; :schedule - Seq of tuples of Quartzite [job trigger]
; :runners - Seq of functions that should be run and kept running.

; NB the Cron-like syntax for Quartz is documented here:
; http://www.quartz-scheduler.org/api/2.2.1/org/quartz/CronExpression.html

(def manifests
  {:hypothesis hypothesis/manifest
   :newsfeed newsfeed/manifest
   :reddit reddit/manifest
   :reddit-links reddit-links/manifest
   :stackexchange stackexchange/manifest
   :twitter twitter/manifest
   :wikipedia wikipedia/manifest
   :wordpressdotcom wordpressdotcom/manifest})

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
   Quartz starts a daemon thread which will block exit."
  [agent-names]
  (log/info "Start scheduler...")
  (let [schedule (-> (qs/initialize) qs/start)]
    (doseq [manifest (manifests-for-names agent-names)]
      (log/info "Adding schedule for" (:agent-name manifest))
      (doseq [[job trigger] (:schedule manifest)]
        (log/info "Add schedule" job trigger)
        (qs/schedule schedule job trigger))))
  (log/info "Finished setting up scheduler."))

(defn run-schedule-once
  "Run the job that would have been scheduled for the given Agent names now, then exit."
  [agent-names]
  (log/info "Running one-off Agent schedule jobs now...")
  (doseq [manifest (manifests-for-names agent-names)]
    (log/info "Running jobs for" (:agent-name manifest))
    (doseq [[job trigger] (:schedule manifest)]
      (log/info "Running job for" (:agent-name manifest) "...")
      (.execute ^org.quartz.Job (.newInstance (.getJobClass job)) nil))
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

