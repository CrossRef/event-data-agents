(ns event-data-agents.checkpoint-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as clj-time]
            [event-data-agents.checkpoint :as checkpoint]
            [event-data-common.storage.memory :as memory]
            [event-data-common.storage.store :as store]))

(deftest hash-identifier
  (testing "Can hash a tuple of strings"
    (let [identifier-a ["reddit" "domain" "xyz.com"]
          identifier-b ["reddit" "domain" "xyz.com"]]

      (is (= (checkpoint/hash-identifier identifier-a)
             (checkpoint/hash-identifier identifier-a)
             (checkpoint/hash-identifier identifier-b))
          "Same input equals itself and identical input.")

      (is (not= (checkpoint/hash-identifier identifier-a)
                (checkpoint/hash-identifier "something else"))
          "Different inputs are not equal"))))

(deftest mark-get-checkpoint
  (testing "get returns previously marked checkpoint"
    (with-redefs [event-data-agents.checkpoint/checkpoint-store (delay (memory/build))]
      (let [identifier ["reddit" "domain" "xyz.com"]
            friday (clj-time/date-time 2016 11 25)
            saturday (clj-time/date-time 2016 11 26)]

        ; Quick sanity assertion.
        (is (not= friday saturday))

        (clj-time/do-at friday
                        (is (= (checkpoint/get-checkpoint identifier)
                               nil)
                            "Getting an empty checkpoint (i.e. first time) returns nil.")

                        (checkpoint/set-checkpoint! identifier)
                        (is (= (checkpoint/get-checkpoint identifier) friday)
                            "Getting checkpoint should return instant on which it was set."))

        (clj-time/do-at saturday
                        (is (= (checkpoint/get-checkpoint identifier) friday)
                            "Getting checkpoint should return instant on which it was set, invariant of when it's called."))

        ; In addition, check this happens on a randomly chosen date (today).
        (is (= (checkpoint/get-checkpoint identifier) friday)
            "Getting checkpoint should return instant on which it was set, invariant of when it's called.")

        (clj-time/do-at saturday
                        (checkpoint/set-checkpoint! identifier)
                        (is (= (checkpoint/get-checkpoint identifier) saturday)
                            "Changing checkpoint should return instant on which it was set."))))))

(deftest floor-date
  (testing "floor-date can be used to constrain a date to a known period"
    (let [long-ago (clj-time/date-time 2010 11 1)
          monday (clj-time/date-time 2016 11 21)
          wednesday (clj-time/date-time 2016 11 23)
          friday (clj-time/date-time 2016 11 25)
          saturday (clj-time/date-time 2016 11 26)]

        ; Quick sanity assertion.
      (is (not= friday saturday))

      (clj-time/do-at saturday
                      (is (= (checkpoint/floor-date nil (clj-time/days 1))
                             friday)
                          "When nil date supplied, return value is period-ago time ago from today's date.")

                      (is (= (checkpoint/floor-date nil (clj-time/days 5))
                             monday)
                          "When nil date supplied, return value is period-ago time ago from today's date.")

                      (is (= (checkpoint/floor-date long-ago (clj-time/days 5))
                             monday)
                          "When date later than period-ago supplied, return value is input date.")

                      (is (= (checkpoint/floor-date wednesday (clj-time/days 5))
                             wednesday)
                          "When date earlier than period-ago supplied, return value is period-ago from today's date.")))))

(deftest has-time-elapsed?
  (testing "has-time-elapsed? returns nil if the checkpoint has been set within the time period"
    (with-redefs [event-data-agents.checkpoint/checkpoint-store (delay (memory/build))]
      (let [identifier ["reddit" "domain" "xyz.com"]
            monday (clj-time/date-time 2016 11 21)
            friday (clj-time/date-time 2016 11 25)]

        (clj-time/do-at monday
                        (checkpoint/set-checkpoint! identifier))

        (clj-time/do-at friday
                        (is (nil? (checkpoint/has-time-elapsed? identifier (clj-time/days 20)))
                            "Should return nil as it was set in the last 20 days.")))))

  (testing "has-time-elapsed? returns the checkpoint date if the if the checkpoint has not been set within the time period"
    (with-redefs [event-data-agents.checkpoint/checkpoint-store (delay (memory/build))]
      (let [identifier ["reddit" "domain" "xyz.com"]
            monday (clj-time/date-time 2016 11 21)
            friday (clj-time/date-time 2016 11 25)]

        (clj-time/do-at monday
                        (checkpoint/set-checkpoint! identifier))

        (clj-time/do-at friday
                        (is (= monday (checkpoint/has-time-elapsed? identifier (clj-time/days 1)))
                            "Should return the date when it was set, as that is before the time interval.")))))

  (testing "has-time-elapsed? returns true if the action has never happened"
    (with-redefs [event-data-agents.checkpoint/checkpoint-store (delay (memory/build))]
      (let [identifier ["reddit" "domain" "xyz.com"]
            friday (clj-time/date-time 2016 11 25)]

        ; Don't set it.

        (clj-time/do-at friday
                        (is (= true (checkpoint/has-time-elapsed? identifier (clj-time/days 1)))
                            "Should return true as it has never been set."))))))

(deftest run-checkpointed
  (testing "Function should be run first time ever"
    (with-redefs [event-data-agents.checkpoint/checkpoint-store (delay (memory/build))]
      (let [identifier ["reddit" "domain" "xyz.com"]
            friday (clj-time/date-time 2016 11 25)
            flag (atom 0)]

        (clj-time/do-at friday
                        (checkpoint/run-checkpointed! identifier (clj-time/days 1) (clj-time/days 1) (fn [_] (swap! flag inc)))
                        (is (= 1 @flag) "Function should have been run once")))))

  (testing "Function should be run if the checkpoint exceeds time"
    (with-redefs [event-data-agents.checkpoint/checkpoint-store (delay (memory/build))]
      (let [identifier ["reddit" "domain" "xyz.com"]
            monday (clj-time/date-time 2016 11 21)
            friday (clj-time/date-time 2016 11 25)
            first-arg (atom nil)
            second-arg (atom nil)
            flag (atom 0)]

        (clj-time/do-at monday
                        (checkpoint/run-checkpointed!
                         identifier
                         (clj-time/days 1)
                         (clj-time/days 10)
                         (fn [last-checkpoint-date]
                           (reset! first-arg last-checkpoint-date)
                           (swap! flag inc)))

                        (is (= 1 @flag) "Function should have been run once first time"))

        (clj-time/do-at friday
                        (checkpoint/run-checkpointed!
                         identifier
                         (clj-time/days 1)
                         (clj-time/days 10)
                         (fn [last-checkpoint-date]
                           (reset! second-arg last-checkpoint-date)
                           (swap! flag inc)))

                        (is (= 2 @flag) "Function should have been run again as it was more than one day since last run"))

        (is (= @first-arg (clj-time/date-time 2016 11 11))
            "First run should have been called with floor-date of 10 days ago")

        (is (= @second-arg monday)
            "Second run should have been called date of first call"))))

  (testing "Function should not be run if there was a more recent execution"
    (with-redefs [event-data-agents.checkpoint/checkpoint-store (delay (memory/build))]
      (let [identifier ["reddit" "domain" "xyz.com"]
            monday (clj-time/date-time 2016 11 21)
            friday (clj-time/date-time 2016 11 25)
            flag (atom 0)]

        (clj-time/do-at monday
                        (checkpoint/run-checkpointed! identifier (clj-time/days 1) (clj-time/days 1) (fn [_] (swap! flag inc)))
                        (is (= 1 @flag) "Function should have been run once first time"))

        (clj-time/do-at friday
                        (checkpoint/run-checkpointed! identifier (clj-time/days 100) (clj-time/days 1) (fn [_] (swap! flag inc)))
                        (is (= 1 @flag) "Function should not have been run again as it was more less than 100 days since last run")

                        (checkpoint/run-checkpointed! identifier (clj-time/days 1) (clj-time/days 1) (fn [_] (swap! flag inc)))
                        (is (= 2 @flag) "Function should have been run again as it was more than one day since last run")

                        (checkpoint/run-checkpointed! identifier (clj-time/days 1) (clj-time/days 1) (fn [_] (swap! flag inc)))
                        (checkpoint/run-checkpointed! identifier (clj-time/days 1) (clj-time/days 1) (fn [_] (swap! flag inc)))
                        (checkpoint/run-checkpointed! identifier (clj-time/days 1) (clj-time/days 1) (fn [_] (swap! flag inc)))
                        (checkpoint/run-checkpointed! identifier (clj-time/days 1) (clj-time/days 1) (fn [_] (swap! flag inc)))

                        (is (= 2 @flag) "Function should have not been run again as there was a recent run."))))))

