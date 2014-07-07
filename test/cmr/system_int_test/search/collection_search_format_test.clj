(ns cmr.system-int-test.search.collection-search-format-test
  "This tests ingesting and searching for collections in different formats."
  (:require [clojure.test :refer :all]
            [cmr.system-int-test.utils.ingest-util :as ingest]
            [cmr.system-int-test.utils.search-util :as search]
            [cmr.system-int-test.utils.index-util :as index]
            [cmr.system-int-test.data2.collection :as dc]
            [cmr.system-int-test.data2.granule :as dg]
            [cmr.system-int-test.data2.core :as d]
            [cmr.system-int-test.utils.url-helper :as url]
            [clj-http.client :as client]
            [cmr.umm.core :as umm]
            [cmr.spatial.mbr :as m]
            [cmr.spatial.codec :as codec]))

(use-fixtures :each (ingest/reset-fixture "PROV1" "PROV2"))

;; Tests that we can ingest and find items in different formats
(deftest multi-format-search-test
  (let [c1-echo (d/ingest "PROV1" (dc/collection {:short-name "S1"
                                                  :version-id "V1"
                                                  :entry-title "ET1"})
                          :echo10)
        c2-echo (d/ingest "PROV2" (dc/collection {:short-name "S2"
                                                  :version-id "V2"
                                                  :entry-title "ET2"})
                          :echo10)
        c3-dif (d/ingest "PROV1" (dc/collection {:entry-id "S3"
                                                 :short-name "S3"
                                                 :version-id "V3"
                                                 :entry-title "ET3"
                                                 :long-name "ET3"})
                         :dif)
        c4-dif (d/ingest "PROV2" (dc/collection {:entry-id "S4"
                                                 :short-name "S4"
                                                 :version-id "V4"
                                                 :entry-title "ET4"
                                                 :long-name "ET4"})
                         :dif)
        all-colls [c1-echo c2-echo c3-dif c4-dif]]
    (index/refresh-elastic-index)

    (testing "Finding refs ingested in different formats"
      (are [search expected]
           (d/refs-match? expected (search/find-refs :collection search))
           {} all-colls
           {:short-name "S4"} [c4-dif]
           {:entry-title "ET3"} [c3-dif]
           {:version ["V3" "V2"]} [c2-echo c3-dif]))

    (testing "Retrieving results in echo10"
      (d/assert-metadata-results-match
        :echo10 all-colls
        (search/find-metadata :collection :echo10 {}))
      (testing "as extension"
        (d/assert-metadata-results-match
          :echo10 all-colls
          (search/find-metadata :collection :echo10 {} {:format-as-ext? true}))))

    (testing "Retrieving results in dif"
      (d/assert-metadata-results-match
        :dif all-colls
        (search/find-metadata :collection :dif {}))
      (testing "as extension"
        (d/assert-metadata-results-match
          :dif all-colls
          (search/find-metadata :collection :dif {} {:format-as-ext? true}))))

    (testing "Retrieving results as XML References"
      (let [refs (search/find-refs :collection {:short-name "S1"})
            location (:location (first (:refs refs)))]
        (is (d/refs-match? [c1-echo] refs))
        (testing "Location allows retrieval of native XML"
          (let [response (client/get location
                                     {:accept :xml
                                      :connection-manager (url/conn-mgr)})]
            (is (= (umm/umm->xml c1-echo :echo10) (:body response))))))

      (testing "as extension"
        (is (d/refs-match? [c1-echo] (search/find-refs :collection
                                                       {:short-name "S1"}
                                                       {:format-as-ext? true})))))))

;; Tests that we can ingest and find difs with spatial and that granules in the dif can also be
;; ingested and found
(deftest dif-with-spatial
  (let [c1 (d/ingest "PROV1" (dc/collection {:spatial-coverage nil}) :dif)
        g1 (d/ingest "PROV1" (dg/granule c1))

        ;; A collection with a granule spatial representation
        c2 (d/ingest "PROV1" (dc/collection {:spatial-coverage (dc/spatial :geodetic)}) :dif)
        g2 (d/ingest "PROV1" (dg/granule c2 {:spatial-coverage (dg/spatial (m/mbr -160 45 -150 35))}))


        ;; A collections with a granule spatial representation and spatial data
        c3 (d/ingest "PROV1"
                     (dc/collection
                       {:spatial-coverage
                        (dc/spatial :geodetic
                                    :geodetic
                                    (m/mbr -10 9 0 -10))})
                     :dif)
        g3 (d/ingest "PROV1" (dg/granule c3))]
    (index/refresh-elastic-index)

    (testing "spatial search for dif collections"
      (are [wnes items]
           (let [found (search/find-refs :collection {:bounding-box (codec/url-encode (apply m/mbr wnes))})
                 matches? (d/refs-match? items found)]
             (when-not matches?
               (println "Expected:" (pr-str (map :entry-title items)))
               (println "Actual:" (->> found :refs (map :name) pr-str)))
             matches?)
           ;; whole world
           [-180 90 180 -90] [c3]
           [-180 90 -11 -90] []
           [-20 20 20 -20] [c3]))

    (testing "spatial search for granules in dif collections"
      (are [wnes items]
           (let [found (search/find-refs :granule {:bounding-box (codec/url-encode (apply m/mbr wnes))})
                 matches? (d/refs-match? items found)]
             (when-not matches?
               (println "Expected:" (pr-str (map :entry-title items)))
               (println "Actual:" (->> found :refs (map :name) pr-str)))
             matches?)
           ;; whole world
           [-180 90 180 -90] [g2]
           [0 90 180 -90] []
           [-180 90 0 -90] [g2]))))


