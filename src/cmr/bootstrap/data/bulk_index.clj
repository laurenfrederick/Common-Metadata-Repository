(ns cmr.bootstrap.data.bulk-index
  "Functions to support concurrent bulk indexing."
  (:require [cmr.common.log :refer (debug info warn error)]
            [cmr.indexer.services.index-service :as index]
            [cmr.metadata-db.data.concepts :as db]
            [clojure.java.jdbc :as j]
            [clj-http.client :as client]
            [clojure.string :as str]
            [cheshire.core :as json]
            [sqlingvo.core :as sql :refer [sql select insert from where with order-by desc delete as]]
            [sqlingvo.vendor :as v]
            [cmr.metadata-db.data.oracle.sql-utils :as su]
            [clojure.core.async :as ca :refer [go go-loop alts!! <!! >!]]
            [cmr.oracle.connection :as oc]
            [cmr.metadata-db.data.oracle.concept-tables :as tables]
            [cmr.transmit.config :as transmit-config]
            [cmr.bootstrap.data.bulk-migration :as bm]))

(defn- get-provider-collection-list
  "Get the list of collecitons belonging to the given provider."
  [system provider-id]
  (let [db (get-in system [:metadata-db :db])
        params {:concept-type :collection
                :provider-id provider-id}
        collections (db/find-concepts db params)]
    (map :concept-id collections)))

(defn- index-granules-for-collection
  "Index the granules for the given collection."
  [system provider-id collection-id]
  (info "Indexing granule data for collection" collection-id)
  (let [db (get-in system [:metadata-db :db])
        params {:concept-type :granule
                :provider-id provider-id
                :parent-collection-id collection-id}
        concept-batches (db/find-concepts-in-batches db params (:db-batch-size system))
        num-granules (index/bulk-index {:system (:indexer system)} concept-batches)]
        (info "Indexed" num-granules "granule(s) for provider" provider-id "collection" collection-id)))

(defn- index-granules-for-provider
  "Index the granule data for every collection for a given provider."
  [system provider-id]
  (info "Indexing granule data for provider" provider-id)
  (let [db (get-in system [:metadata-db :db])
        params {:concept-type :granule
                :provider-id provider-id}
        concept-batches (db/find-concepts-in-batches db params (:db-batch-size system))
        num-granules (index/bulk-index {:system (:indexer system)} concept-batches)]
        (info "Indexed" num-granules "granule(s) for provider" provider-id)))


(defn- index-provider-collections
  "Index all the collections concepts for a given provider."
  [system provider-id]
  (let [db (get-in system [:metadata-db :db])
        params {:concept-type :collection
                :provider-id provider-id}
        concept-batches (db/find-concepts-in-batches db params (:db-batch-size system))]
    (index/bulk-index {:system (:indexer system)} concept-batches)))


(defn- unindex-provider
  "Remove all records from elastic related to the given provider including indexes."
  [system provider-id]
  ;; FIXME - Implement this when done with bulk migration
  )

(defn- index-provider
  "Bulk index a provider."
  [system provider-id]
  (info "Indexing provider" provider-id)
  (unindex-provider system provider-id)
  (index-provider-collections system provider-id)
  (index-granules-for-provider system provider-id)
  (info "Indexing of provider" provider-id "completed."))

;; Background task to handle provider bulk index requests
(defn handle-bulk-index-requests
  "Handle any requests for indexing providers."
  [system]
  (info "Starting background task for monitoring bulk provider indexing channels.")
  (let [channel (:provider-index-channel system)]
    (ca/thread (while true
                 (try ; catch any errors and log them, but don't let the thread die
                   (let [provider-id (<!! channel)]
                     (index-provider system provider-id))
                   (catch Throwable e
                     (error e (.getMessage e)))))))
  (let [channel (:collection-index-channel system)]
    (ca/thread (while true
                 (try ; catch any errors and log them, but don't let the thread die
                   (let [[provider-id collection-id] (<!! channel)]
                     (index-granules-for-collection system provider-id collection-id))
                   (catch Throwable e
                     (error e (.getMessage e)))))))
  )

