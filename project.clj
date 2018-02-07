(defproject event-data-agents "1.1.10"
  :description "Crossref Event Data Agents"
  :url "http://eventdata.crossref.org"
  :license {:name "The MIT License (MIT)"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[clj-http "2.3.0"]
                 [clj-time "0.12.2"]
                 [com.rometools/rome "1.6.1"]
                 [commons-codec/commons-codec "1.10"]
                 [crossref-util "0.1.10"]
                 [event-data-common "0.1.41"]
                 [org.apache.commons/commons-lang3 "3.5"]
                 [org.apache.httpcomponents/httpclient "4.5.3"]
                 [org.apache.kafka/kafka-clients "0.10.2.0"]
                 [org.apache.kafka/kafka-streams "0.10.2.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.391"]
                 [org.clojure/data.csv "0.1.4"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/data.zip "0.1.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [org.jsoup/jsoup "1.10.1"]
                 [robert/bruce "0.8.0"]
                 [slingshot "0.12.2"]
                 [throttler "1.0.0"]
                 [yogthos/config "0.8"]
                 [overtone/at-at "1.2.0"]
                 [twitter-api "1.8.0"]
                 [org.glassfish.jersey.media/jersey-media-sse "2.26"]
                 [org.glassfish.jersey.inject/jersey-hk2 "2.26"]]
  :jvm-opts ["-Duser.timezone=UTC" "-Xmx3G"]
  :plugins [[lein-cljfmt "0.5.7"]]
  :main ^:skip-aot event-data-agents.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
