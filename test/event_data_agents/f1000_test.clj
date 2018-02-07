(ns event-data-agents.f1000-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as clj-time]
            [clojure.java.io :as io]
            [event-data-agents.agents.f1000.core :as f1000]))

(deftest parse-actions
  (testing "Actions should be parsed out of Agent input."
    (let [actions (f1000/actions-from-stream (io/reader "resources/test/f1000.xml") (clj-time/date-time 1900))]
      (is
       (=
        (map #(dissoc % :id) actions)
        [{:relation-type-id "reviews",
          :occurred-at "2005-11-15T00:00:00Z",
          :url "https://doi.org/10.3410/f.1012.458502",
          :subj
          {:pid "https://doi.org/10.3410/f.1012.458502",
           :alternative_id "458502",
           :work_type_id "review"},
          :observations
          [{:type :url, :input-url "https://doi.org/10.1038/sj.bjc.6601743"}]}
         {:relation-type-id "reviews",
          :occurred-at "2005-11-15T00:00:00Z",
          :url "https://doi.org/10.3410/f.1018.458508",
          :subj
          {:pid "https://doi.org/10.3410/f.1018.458508",
           :alternative_id "458508",
           :work_type_id "review"},
          :observations
          [{:type :url, :input-url "https://doi.org/10.1073/pnas.0503069102"}]}
         {:relation-type-id "reviews",
          :occurred-at "2006-02-03T00:00:00Z",
          :url "https://doi.org/10.3410/f.1018.466576",
          :subj
          {:pid "https://doi.org/10.3410/f.1018.466576",
           :alternative_id "466576",
           :work_type_id "review"},
          :observations
          [{:type :url, :input-url "https://doi.org/10.1073/pnas.0503069102"}]}]))

      "Actions should be deterministically derived from the input."))

  (testing "Actions should be filtered by date."
    (let [actions (f1000/actions-from-stream (io/reader "resources/test/f1000.xml") (clj-time/date-time 2005 11 16))]
      (is
       (=
        (map #(dissoc % :id) actions)
        [{:relation-type-id "reviews",
          :occurred-at "2006-02-03T00:00:00Z",
          :url "https://doi.org/10.3410/f.1018.466576",
          :subj
          {:pid "https://doi.org/10.3410/f.1018.466576",
           :alternative_id "466576",
           :work_type_id "review"},
          :observations
          [{:type :url, :input-url "https://doi.org/10.1073/pnas.0503069102"}]}]))

      "Only those actions that occur after the date are returned.")))

