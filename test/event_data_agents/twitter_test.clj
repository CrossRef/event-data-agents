(ns event-data-agents.twitter-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as clj-time]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [event-data-agents.agents.twitter.core :as twitter])
  (:import [org.apache.commons.codec.digest DigestUtils]))

(deftest tweet-url->tweet-id
  (testing "tweet-url->tweet-id should extract Tweet ID from Tweet URL.
                   https://help.twitter.com/en/managing-your-account/twitter-username-rules"
           (is (= "123456789"
                  (twitter/tweet-url->tweet-id "http://twitter.com/jim/statuses/123456789")
                  (twitter/tweet-url->tweet-id "http://twitter.com/_/statuses/123456789")
                  (twitter/tweet-url->tweet-id "http://twitter.com/987654321/statuses/123456789")))))

(deftest tweet-url->uri
  (testing "tweet-url->uri should build a URI."
           (is (= "twitter://status?id=123456789" (twitter/tweet-url->uri "http://twitter.com/jim/statuses/123456789")))))


(deftest actor-url->username
  (testing "actor-url->username should extract the username"
           (is (= "jim" (twitter/actor-url->username "http://www.twitter.com/jim")))
           (is (= "12345" (twitter/actor-url->username "http://www.twitter.com/12345")))))

(deftest actor-url->uri
  (testing "actor-url->uri should build a URI."
           (is (= "twitter://user?screen_name=fred" (twitter/actor-url->uri "http://www.twitter.com/fred")))
           (is (= "twitter://user?screen_name=12345" (twitter/actor-url->uri "http://www.twitter.com/12345")))))


(def tweet-id "1111111111")

(def author-name "jeremiahcarberry")

(def author-id "3333333333")

(def body-text "THIS IS THE BODY TEXT")

(def matched-rule-id [4444444444, 5555555555])

(def input
  "Typical input from Gnip."
  "{\"id\":\"tag:search.twitter.com,2005:1111111111\",\"objectType\":\"activity\",\"verb\":\"post\",\"postedTime\":\"2017-02-13T18:04:47.000Z\",\"generator\":{\"displayName\":\"Twitter Web Client\",\"link\":\"http:\\/\\/twitter.com\"},\"provider\":{\"objectType\":\"service\",\"displayName\":\"Twitter\",\"link\":\"http:\\/\\/www.twitter.com\"},\"link\":\"http:\\/\\/twitter.com\\/jeremiahcarberry\\/statuses\\/1111111111\",\"body\":\"THIS IS THE BODY TEXT\",\"actor\":{\"objectType\":\"person\",\"id\":\"id:twitter.com:3333333333\",\"link\":\"http:\\/\\/www.twitter.com\\/jeremiahcarberry\",\"displayName\":\"XXXXX\",\"postedTime\":\"2015-11-24T19:16:56.006Z\",\"image\":\"http://example.com/image.png\",\"summary\":null,\"friendsCount\":999,\"followersCount\":999,\"listedCount\":999,\"statusesCount\":999,\"twitterTimeZone\":\"Pacific Time (US & Canada)\",\"verified\":false,\"utcOffset\":\"-28800\",\"preferredUsername\":\"jeremiahcarberry\",\"languages\":[\"en\"],\"links\":[{\"href\":null,\"rel\":\"me\"}],\"favoritesCount\":999},\"object\":{\"objectType\":\"note\",\"id\":\"object:search.twitter.com,2005:1111111111\",\"summary\":\"THIS IS THE BODY TEXT\",\"link\":\"http:\\/\\/twitter.com\\/jeremiahcarberry\\/statuses\\/1111111111\",\"postedTime\":\"2017-02-13T18:04:47.000Z\"},\"favoritesCount\":0,\"twitter_entities\":{\"hashtags\":[{\"text\":\"XXXXX\",\"indices\":[0,5]}],\"urls\":[{\"url\":\"https://exampleshort.com/1\",\"expanded_url\":\"http://examplelong.com/1\",\"display_url\":\"http://example.com/display_url\",\"indices\":[115,138]}],\"user_mentions\":[],\"symbols\":[]},\"twitter_lang\":\"en\",\"retweetCount\":0,\"gnip\":{\"matching_rules\":[{\"tag\":null,\"id\":4444444444},{\"tag\":null,\"id\":5555555555}],\"urls\":[{\"url\":\"http://exampleshort.com/1\",\"expanded_url\":\"http://examplelong.com/1\",\"expanded_status\":200,\"expanded_url_title\":\"URL-TITLE\"}]},\"twitter_filter_level\":\"low\"}")

(deftest can-parse
  (testing "entry->action can parse an standard entry to generate Percolator Actions"
           (let [result (-> input (json/read-str :key-fn keyword) twitter/entry->action)

                 ; Expect the Tweet to be represented as a compliant URI.
                 expected-url "twitter://status?id=1111111111"

                 ; But the ID hash should still be based on the input URL.
                 original-tweet-url "http://twitter.com/jeremiahcarberry/statuses/1111111111"
                 expected-id (DigestUtils/sha1Hex ^String original-tweet-url)]

             (is (= result
                    ; ID should be hash of the Tweet URL.
                    {:id expected-id
                     :occurred-at "2017-02-13T18:04:47Z",
                     :url expected-url
                     ; Both rule IDs should be extracted.
                     :extra {:gnip-matching-rules [4444444444 5555555555]}
                     :subj
                     ; Title should include tweet ID but not the text.
                     {:title "Tweet 1111111111"
                      ; Issued date should be carried through.
                      :issued "2017-02-13T18:04:47.000Z"

                      ; Author ID URL should be transformed to URI.
                      :author {:url "twitter://user?screen_name=jeremiahcarberry"}
                      :original-tweet-url
                      "twitter://status?id=1111111111"
                      :original-tweet-author nil
                      :alternative-id "1111111111"}
                     :relation-type-id "discusses"
                     :observations
                     ; Plain text of tweet should be extracted BUT should have the sensitive flag set.
                     [{:type "plaintext" :input-content "THIS IS THE BODY TEXT" :sensitive true}
                      ; All URLs, including both the original and the expanded, should have
                      {:type "url" :sensitive false :input-url "http://examplelong.com/1"}
                      {:type "url" :sensitive false :input-url "http://exampleshort.com/1"}]})))))

(deftest tweet-id-from-url
  (testing "tweet-id-from-url can extract the tweet ID from an input Tweet ID URL."
           (is (=
                "1111111111"
                (twitter/tweet-url->tweet-id "http://twitter.com/jeremiahcarberry/statuses/1111111111")))))

(deftest parse-accept-errors
  (testing "entry->action can accept Gnip entry"
           (let [input "{\"error\":{\"message\":\"This stream is currently at the maximum allowed connection limit\",\"sent\":\"2017-02-14T12:37:11+00:00\",\"transactionId\":\"00ac7eab00fa8ed0\"}}"]
             (is (nil? (-> input (json/read-str :key-fn keyword) twitter/entry->action)) "Exception should not be raised on expected error, instead return nil."))))

(deftest parse-empty-urls
  (testing "If empty URLs are passed in, no nil observations are created."
    (let [input-missing-urls "{\"id\":\"tag:search.twitter.com,2005:1111111111\",\"objectType\":\"activity\",\"verb\":\"post\",\"postedTime\":\"2017-02-13T18:04:47.000Z\",\"generator\":{\"displayName\":\"Twitter Web Client\",\"link\":\"http:\\/\\/twitter.com\"},\"provider\":{\"objectType\":\"service\",\"displayName\":\"Twitter\",\"link\":\"http:\\/\\/www.twitter.com\"},\"link\":\"http:\\/\\/twitter.com\\/jeremiahcarberry\\/statuses\\/1111111111\",\"body\":\"THIS IS THE BODY TEXT\",\"actor\":{\"objectType\":\"person\",\"id\":\"id:twitter.com:3333333333\",\"link\":\"http:\\/\\/www.twitter.com\\/jeremiahcarberry\",\"displayName\":\"XXXXX\",\"postedTime\":\"2015-11-24T19:16:56.006Z\",\"image\":\"http://example.com/image.png\",\"summary\":null,\"friendsCount\":999,\"followersCount\":999,\"listedCount\":999,\"statusesCount\":999,\"twitterTimeZone\":\"Pacific Time (US & Canada)\",\"verified\":false,\"utcOffset\":\"-28800\",\"preferredUsername\":\"jeremiahcarberry\",\"languages\":[\"en\"],\"links\":[{\"href\":null,\"rel\":\"me\"}],\"favoritesCount\":999},\"object\":{\"objectType\":\"note\",\"id\":\"object:search.twitter.com,2005:1111111111\",\"summary\":\"THIS IS THE BODY TEXT\",\"link\":\"http:\\/\\/twitter.com\\/jeremiahcarberry\\/statuses\\/1111111111\",\"postedTime\":\"2017-02-13T18:04:47.000Z\"},\"favoritesCount\":0,\"twitter_entities\":{\"hashtags\":[{\"text\":\"XXXXX\",\"indices\":[0,5]}],\"urls\":[{\"url\":null,\"expanded_url\":null,\"display_url\":\"http://example.com/display_url\",\"indices\":[115,138]}],\"user_mentions\":[],\"symbols\":[]},\"twitter_lang\":\"en\",\"retweetCount\":0,\"gnip\":{\"matching_rules\":[{\"tag\":null,\"id\":4444444444},{\"tag\":null,\"id\":5555555555}],\"urls\":[{\"url\":null,\"expanded_url\":null,\"expanded_status\":200,\"expanded_url_title\":\"URL-TITLE\"}]},\"twitter_filter_level\":\"low\"}"
          result (-> input-missing-urls (json/read-str :key-fn keyword) twitter/entry->action)]

      ; Plain text of tweet should be extracted BUT should have the sensitive flag set.
      ; Both the URLs were null, so they shouldn't be included.
      (is (= (:observations result)
             [{:type "plaintext" :input-content "THIS IS THE BODY TEXT" :sensitive true}])
          "When URLs are missing from input, no URL observations are generated."))))
