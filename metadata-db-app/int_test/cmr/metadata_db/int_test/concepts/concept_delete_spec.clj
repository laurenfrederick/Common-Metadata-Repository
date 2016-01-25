(ns cmr.metadata-db.int-test.concepts.concept-delete-spec
  "Defines a common set of tests for deleting concepts."
  (:require [clojure.test :refer :all]
            [clj-time.core :as t]
            [cmr.common.concepts :as cc]
            [cmr.metadata-db.int-test.utility :as util]
            [cmr.metadata-db.int-test.concepts.concept-save-spec :as c-spec]))

(defn general-delete-test
  "Delete tests that all concepts should pass"
  [concept-type provider-ids]
  (doseq [provider-id provider-ids]
    (testing "with delete endpoint"
      (let [concept1 (c-spec/gen-concept concept-type provider-id 1 {})
            concept2 (c-spec/gen-concept concept-type provider-id 2 {})
            {:keys [concept-id] :as saved-concept1} (util/save-concept concept1 3)
            {concept-id2 :concept-id revision-id2 :revision-id} (util/save-concept concept2)
            {:keys [status revision-id] :as tombstone} (util/delete-concept concept-id)
            deleted-concept1 (:concept (util/get-concept-by-id-and-revision concept-id revision-id))
            saved-concept1 (:concept (util/get-concept-by-id-and-revision concept-id (dec revision-id)))]
        (is (= {:status 201 :revision-id 4}
               {:status status :revision-id revision-id}))

        (is (= (dissoc (assoc saved-concept1
                              :deleted true
                              :metadata ""
                              :revision-id revision-id
                              :user-id nil)
                       :revision-date :user-id :transaction-id)
               (dissoc deleted-concept1 :revision-date :user-id :transaction-id)))

        ;; Make sure that a deleted concept gets it's own unique revision date
        (is (t/after? (:revision-date deleted-concept1) (:revision-date saved-concept1))
            "The deleted concept revision date should be after the previous revisions revision date.")

        (is (= (util/delete-concept concept-id)
               (util/delete-concept concept-id)
               (util/delete-concept concept-id)))

        ;; Other data left in database
        (util/verify-concept-was-saved (assoc concept2 :concept-id concept-id2 :revision-id revision-id2))))

    (testing "delete with valid revision"
      (let [concept (c-spec/gen-concept concept-type provider-id 3 {})
            {:keys [concept-id]} (util/save-concept concept)
            {:keys [status revision-id]} (util/delete-concept concept-id 4)]
        (is (= {:status 201
                :revision-id 4}
               {:status status
                :revision-id revision-id}))))

    (testing "delete with skipped revision"
      (let [concept (c-spec/gen-concept concept-type provider-id 4 {})
            {:keys [concept-id]} (util/save-concept concept)
            {:keys [status revision-id]} (util/delete-concept concept-id 100)]
        (is (= {:status 201
                :revision-id 100}
               {:status status
                :revision-id revision-id}))))

    (testing "delete with new revision"
      (let [concept (c-spec/gen-concept concept-type provider-id 5 {})
            {:keys [concept-id]} (util/save-concept concept)
            {status1 :status revision-id1 :revision-id} (util/delete-concept concept-id 5)
            {status2 :status revision-id2 :revision-id} (util/delete-concept concept-id 7)]
        (is (= 201 status1 status2))
        (is (= 5 revision-id1))
        (is (= 7 revision-id2))))

    (testing "save tombstone"
      (let [concept1 (c-spec/gen-concept concept-type provider-id 6 {})
            concept2 (c-spec/gen-concept concept-type provider-id 7 {})
            con1 (util/save-concept concept1 3)
            {concept-id2 :concept-id revision-id2 :revision-id} (util/save-concept concept2)

            {:keys [status revision-id]} (util/save-concept {:concept-id (:concept-id con1)
                                                             :deleted true})
            stored-concept1 (:concept (util/get-concept-by-id-and-revision (:concept-id con1) revision-id))]
        (is (= {:status 201
                :revision-id 4
                :deleted true
                :metadata ""}
               {:status status
                :revision-id revision-id
                :deleted (:deleted stored-concept1)
                :metadata (:metadata stored-concept1)}))

        ;; Other data left in database
        (util/verify-concept-was-saved
          (assoc concept2 :concept-id concept-id2 :revision-id revision-id2))))))

(defn general-force-delete-test
  "Force delete tests that all concepts should pass"
  [concept-type provider-ids]
  (doseq [provider-id provider-ids]
    (let [concept (c-spec/gen-concept concept-type provider-id 1 {})
          saved-concept (util/save-concept concept)
          concept-id (:concept-id saved-concept)
          _ (dorun (repeatedly 3 #(util/save-concept concept)))]
      (testing "force-delete"
        (let [{:keys [status revision-id]} (util/force-delete-concept concept-id 2)]
          (testing "revision-id correct"
            (is (= status 200))
            (is (= revision-id 2)))
          (testing "revision is gone"
            (is (= 404 (:status (util/get-concept-by-id-and-revision concept-id 2)))))
          (testing "earlier revisions still available"
            (util/verify-concept-was-saved (assoc concept :concept-id concept-id :revision-id 1)))
          (testing "later revisions still available"
            (util/verify-concept-was-saved (assoc concept :concept-id concept-id :revision-id 3))
            (util/verify-concept-was-saved (assoc concept :concept-id concept-id :revision-id 4)))
          (testing "delete non-existent revision gets 404"
            (is (= 404 (:status (util/force-delete-concept concept-id 2))))
            (is (= 404 (:status (util/force-delete-concept concept-id 22))))))))))
