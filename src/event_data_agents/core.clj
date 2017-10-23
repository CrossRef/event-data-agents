(ns event-data-agents.core
  (:require [event-data-agents.agents :as agents]
            [clojure.tools.logging :as log]
            [overtone.at-at :as at-at])
  (:gen-class))

(def schedule-pool (at-at/mk-pool))

(defn setup-thread-handler
  "Install default thread exception handler to log and exit."
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread ex]
        (log/error ex "Uncaught exception on" (.getName thread))
        (log/error "Exiting from thread handler NOW.")
        (System/exit 1)))))

(defn -main
  [& args]

  (setup-thread-handler)
  ; Always install a heartbeat.
  (at-at/every 10000 #(log/info "Tick") schedule-pool)
  (let [command (first args)
        agent-names (rest args)]
    (condp = command
      "start-schedule" (agents/start-schedule schedule-pool agent-names)
      "run-schedule-once" (agents/run-schedule-once agent-names)
      "run-daemons" (agents/run-daemons agent-names)
      (do (log/error "Didn't recognise command" command)
          (System/exit 1)))))
