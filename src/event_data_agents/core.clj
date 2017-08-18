(ns event-data-agents.core
  (:require [event-data-agents.agents :as agents]
            [clojure.tools.logging :as log])
  (:gen-class))



(defn -main
  "I don't do a whole lot ... yet."
    [& args]
  (let [command (first args)
        agent-names (rest args)]
    (condp = command
      "start-schedule" (agents/start-schedule agent-names)
      "run-schedule-once" (agents/run-schedule-once agent-names)
      "run-daemons" (agents/run-daemons agent-names)
      (log/error "Didn't recognise command" command))))


