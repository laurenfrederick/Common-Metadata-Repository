(ns cmr.system-int-test.search.collection-relevancy.collection-usage-relevancy
  "This tests the CMR Search API's community usage relevancy scoring and ranking
  capabilities"
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [cmr.access-control.test.util :as u]
   [cmr.common.config :as config]
   [cmr.common.util :as util :refer [are3]]
   [cmr.mock-echo.client.echo-util :as e]
   [cmr.search.data.query-to-elastic :as query-to-elastic]
   [cmr.system-int-test.data2.umm-spec-collection :as data-umm-c]
   [cmr.system-int-test.data2.collection :as dc]
   [cmr.system-int-test.data2.core :as d]
   [cmr.system-int-test.system :as s]
   [cmr.system-int-test.utils.dev-system-util :as dev-sys-util]
   [cmr.system-int-test.utils.humanizer-util :as hu]
   [cmr.system-int-test.utils.index-util :as index]
   [cmr.system-int-test.utils.ingest-util :as ingest]
   [cmr.system-int-test.utils.search-util :as search]
   [cmr.system-int-test.utils.search-util :as search]))

(use-fixtures :each (ingest/reset-fixture {"provguid1" "PROV1"}))

(def sample-usage-csv
  (str "Product,Version,Hosts\n"
       "AMSR-L1A,3,10\n"
       "AG_VIRTUAL,1,100\n"
       "AG_MAPSS,2,30\n"
       "AST_05,B,8\n"))

(deftest community-usage-relevancy-scoring
  (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! true))
  (hu/ingest-community-usage-metrics sample-usage-csv)
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "Relevancy 1"
                                                        :Version "3"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_VIRTUAL"
                                                        :EntryTitle "Relevancy 2"
                                                        :Version "1"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_MAPSS"
                                                        :EntryTitle "Relevancy 3"
                                                        :Version "2"}))
  (index/wait-until-indexed)

  (testing "Equal keyword relevancy, order by usage"
    (let [results (:refs (search/find-refs :collection {:keyword "Relevancy"}))]
      (is (= ["Relevancy 2" "Relevancy 3" "Relevancy 1"] (map :name results)))))

  (testing "Collection missing from metrics file"
    (d/ingest-umm-spec-collection "PROV1"
                                  (data-umm-c/collection {:ShortName "AG_MAPSS"
                                                          :EntryTitle "Relevancy 4"
                                                          :Version "5"}))
    (index/wait-until-indexed)

    (let [results (:refs (search/find-refs :collection {:keyword "Relevancy"}))]
      (is (= "Relevancy 4" (:name (last results))))))

  (testing "Turn off using relevancy score"
    (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! false))
    (let [results (:refs (search/find-refs :collection {:keyword "Relevancy"}))]
      (is (= ["Relevancy 4" "Relevancy 3" "Relevancy 2" "Relevancy 1"] (map :name results))))))

(deftest keyword-relevancy-takes-precedence
  (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! true))
  (hu/ingest-community-usage-metrics sample-usage-csv)
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "Relevancy 1"
                                                        :Version "3"
                                                        :Platforms [(data-umm-c/platform {:ShortName "Relevancy"})]}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AST_05"
                                                        :EntryId "Relevancy"
                                                        :EntryTitle "AST_05"
                                                        :Version "B"
                                                        :Projects (data-umm-c/projects "Relevancy")
                                                        :Platforms [(data-umm-c/platform {:ShortName "Relevancy"})]}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_VIRTUAL"
                                                        :EntryTitle "Relevancy 2"
                                                        :Version "1"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_MAPSS"
                                                        :EntryTitle "Relevancy 3"
                                                        :Version "2"}))
  (index/wait-until-indexed)
  (let [results (:refs (search/find-refs :collection {:keyword "Relevancy"}))]
    (is (= ["AST_05" "Relevancy 1" "Relevancy 2" "Relevancy 3"] (map :name results))))

  (testing "Usage sort takes precedence over keyword Relevancy"
    (let [results (:refs (search/find-refs :collection {:keyword "Relevancy" :sort-key "-usage-score"}))]
      (is (= ["Relevancy 2" "Relevancy 3" "Relevancy 1" "AST_05"]
             (map :name results))))))

(deftest ingest-metrics-after-collections
  (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! true))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "Relevancy 1"
                                                        :Version "3"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_VIRTUAL"
                                                        :EntryTitle "Relevancy 2"
                                                        :Version "1"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_MAPSS"
                                                        :EntryTitle "Relevancy 3"
                                                        :Version "2"}))
  (index/wait-until-indexed)
  (hu/ingest-community-usage-metrics sample-usage-csv)

  (let [results (:refs (search/find-refs :collection {:keyword "Relevancy"}))]
    (is (= ["Relevancy 2" "Relevancy 3" "Relevancy 1"] (map :name results)))))

(deftest change-metrics
  (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! true))
  (hu/ingest-community-usage-metrics sample-usage-csv)

  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "Relevancy 1"
                                                        :Version "3"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_VIRTUAL"
                                                        :EntryTitle "Relevancy 2"
                                                        :Version "1"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_MAPSS"
                                                        :EntryTitle "Relevancy 3"
                                                        :Version "2"}))
  (index/wait-until-indexed)

  ;; Ingest new community usage metrics and check that results change
  (hu/ingest-community-usage-metrics (str "Product,Version,Hosts\n"
                                          "AMSR-L1A,3,40\n"
                                          "AG_VIRTUAL,1,12\n"
                                          "AG_MAPSS,2,58\n"))

  (let [results (:refs (search/find-refs :collection {:keyword "Relevancy"}))]
    (is (= ["Relevancy 3" "Relevancy 1" "Relevancy 2"] (map :name results)))))

;; Outside of keyword search, allow the user to sort by community usage
(deftest sort-by-community-usage
  (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! true))
  (hu/ingest-community-usage-metrics sample-usage-csv)
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A" ;10
                                                        :EntryTitle "Relevancy 1"
                                                        :Version "3"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_VIRTUAL" ; 100
                                                        :EntryTitle "Relevancy 2"
                                                        :Version "1"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_MAPSS" ;30
                                                        :EntryTitle "Relevancy 3"
                                                        :Version "2"
                                                        :Platforms [(data-umm-c/platform {:ShortName "Relevancy"})]}))
  (index/wait-until-indexed)
  (testing "Sort by usage ascending"
    (let [results (:refs (search/find-refs :collection {:sort-key "usage-score"}))]
      (is (= ["Relevancy 1" "Relevancy 3" "Relevancy 2"]
             (map :name results)))))
  (testing "Sort by usage descending"
    (let [results (:refs (search/find-refs :collection {:sort-key "-usage-score"}))]
      (is (= ["Relevancy 2" "Relevancy 3" "Relevancy 1"]
             (map :name results))))))

;; More complicated example/test with entries with version N/A - N/A version entries are
;; not used as part of the access count
(def sample-csv-not-provided-versions
  (str "Product,Version,Hosts\n"
       "AMSR-L1A,3,10\n"
       "AMSR-L1A,N/A,50\n" ;; Essentially access count of 0, since 'N/A' version
       "AG_VIRTUAL,1,100\n"
       "AG_MAPSS,2,30\n"))

(deftest community-usage-not-provided-versions
  (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! true))
  (hu/ingest-community-usage-metrics sample-csv-not-provided-versions)
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "AMSR-L1A V3 Relevancy"
                                                        :Version "3"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "AMSR-L1A V2 Relevancy"
                                                        :Version "2"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_VIRTUAL"
                                                        :EntryTitle "AG_VIRTUAL Relevancy"
                                                        :Version "1"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AG_MAPSS"
                                                        :EntryTitle "AG_MAPSS Relevancy"
                                                        :Version "2"}))
  (index/wait-until-indexed)
  (let [results (:refs (search/find-refs :collection {:keyword "Relevancy"}))]
    (is (= ["AG_VIRTUAL Relevancy" "AG_MAPSS Relevancy" "AMSR-L1A V3 Relevancy" "AMSR-L1A V2 Relevancy"]
           (map :name results)))))

(deftest community-usage-version-formatting
  (dev-sys-util/eval-in-dev-sys `(query-to-elastic/set-sort-use-relevancy-score! true))
  (hu/ingest-community-usage-metrics (str "Product,Version,Hosts\n"
                                          "AMSR-L1A,1,10\n"
                                          "AMSR-L1A,3,100\n"
                                          "AMSR-L1A,002,50\n"))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "AMSR-L1A 003"
                                                        :Version "003"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "AMSR-L1A 1"
                                                        :Version "1"}))
  (d/ingest-umm-spec-collection "PROV1"
                                (data-umm-c/collection {:ShortName "AMSR-L1A"
                                                        :EntryTitle "AMSR-L1A 2"
                                                        :Version "2"}))
  (index/wait-until-indexed)
  (let [results (:refs (search/find-refs :collection {:keyword "AMSR-L1A"}))]
    (is (= ["AMSR-L1A 003" "AMSR-L1A 2" "AMSR-L1A 1"]
           (map :name results)))))
