(ns cmr.ingest.services.bulk-update-service
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as io]
   [cmr.common.services.errors :as errors]
   [cmr.common.validations.json-schema :as js]
   [cmr.ingest.data.bulk-update :as data-bulk-update]
   [cmr.ingest.data.ingest-events :as ingest-events]
   [cmr.transmit.metadata-db2 :as mdb2]

   [cmr.common.xml :as cx]
   [clojure.data.xml :as xml2]
   [cmr.common.xml.simple-xpath :as xpath]))


(def bulk-update-schema
  (js/json-string->json-schema (slurp (io/resource "bulk_update_schema.json"))))

(defn validate-bulk-update-post-params
  "Validate post body for bulk update. Validate against schema and validation
  rules."
  [json]
  (js/validate-json! bulk-update-schema json)
  (let [body (json/parse-string json true)
        {:keys [update-type update-value find-value]} body]
    (when (and (not= "CLEAR_FIELD" update-type)
               (not= "FIND_AND_REMOVE" update-type)
               (nil? update-value))
      (errors/throw-service-errors :bad-request
                                   [(format "An update value must be supplied when the update is of type %s"
                                            update-type)]))
    (when (and (or (= "FIND_AND_REPLACE" update-type)
                   (= "FIND_AND_REMOVE" update-type))
               (nil? find-value))
      (errors/throw-service-errors :bad-request
                                   [(format "A find value must be supplied when the update is of type %s"
                                            update-type)]))))

(defn validate-and-save-bulk-update
  "Validate the bulk update POST parameters, save rows to the db for task
  and collection statuses, and queueu bulk update. Return task id, which comes
  from the db save."
  [context provider-id json]
  (validate-bulk-update-post-params json)
  (let [bulk-update-params (json/parse-string json true)
        {:keys [concept-ids]} bulk-update-params
        ;; Write db rows - one for overall status, one for each concept id
        task-id (data-bulk-update/create-bulk-update-task context
                 provider-id json concept-ids)]
    ;; Queue the bulk update event
    (ingest-events/publish-ingest-event context
      (ingest-events/ingest-bulk-update-event task-id bulk-update-params))
    task-id))

(defn handle-bulk-update-event
  "For each concept-id, queueu collection bulk update messages"
  [context task-id bulk-update-params]
  (let [{:keys [concept-ids]} bulk-update-params]
    (doseq [concept-id concept-ids]
     (ingest-events/publish-ingest-event context
       (ingest-events/ingest-collection-bulk-update-event
         task-id concept-id bulk-update-params)))))

(defn handle-collection-bulk-update-event
  "Perform update for the given concept id. Log an error status of the concept
  cannot be found."
  [context task-id concept-id bulk-update-params]
  (try
    (if-let [concept (mdb2/get-latest-concept context concept-id)]
      (data-bulk-update/update-bulk-update-task-collection-status context task-id concept-id "COMPLETE" nil)
      (data-bulk-update/update-bulk-update-task-collection-status context task-id
        concept-id "FAILED" (format "Concept-id [%s] is not valid." concept-id)))
    (catch Exception e
      (let [message (.getMessage e)
            concept-id-message (re-find #"Concept-id.*is not valid." message)]
        (if concept-id-message
          (data-bulk-update/update-bulk-update-task-collection-status context task-id concept-id "FAILED" concept-id-message)
          (data-bulk-update/update-bulk-update-task-collection-status context task-id concept-id "FAILED" message))))))


(comment
 (def doc
  (xml2/parse (java.io.StringReader. (slurp (io/resource "example_data/echo10/C179001707-SEDAC.xml")))))

 ;; correct
 (def keywords (cx/elements-at-path doc [:ScienceKeywords :ScienceKeyword]))
 (first keywords)

 (cx/element-at-path (first keywords) [:CategoryKeyword])

 (filter #(= "EARTH SCIENCE" (cx/string-at-path % [:CategoryKeyword])) keywords)

 (defn- update-element
  [element val]
  (assoc element :content val))

 (cx/update-elements-at-path (first keywords) [:ScienceKeywords :ScienceKeyword] update-element)

 (defn- element-updates
  [element conditions updates]
  (if (every? true?
       (mapv (fn [condition]
              (= (first (vals condition)) (cx/string-at-path element [(first (keys condition))])))
            conditions))
   (let [atom-element (atom element)]
    (doseq [key (keys updates)
            :let [val (get updates key)]]
     (reset! atom-element (cx/update-elements-at-path @atom-element [key] update-element val)))
    @atom-element)
   element))

 (def condition (first conditions))

 (element-updates (first keywords) [{:CategoryKeyword "EARTH SCIENCE"}
                                    {:TopicKeyword "AGRICULTURE"}]
   {:CategoryKeyword "LAUREN"
    :TopicKeyword "Functions"})


 (def update-sk
   #clojure.data.xml.Element
    {:tag :ScienceKeyword,
     :attrs {}
     :content
       [#clojure.data.xml.Element
         {:tag :CategoryKeyword
          :attrs {}
          :content ["TEST CAT"]}]})

 ;; ADD TO EXISTING
 (defn update-kws
  [keywords val]
  (update keywords :content #(conj % val)))

 (def sk-element
  (cx/element-at-path doc [:ScienceKeywords]))

 (def doc2
  (cx/update-elements-at-path doc [:ScienceKeywords] update-kws update-sk))

 ;; CLEAR ALL AND replace
 (defn clear-and-replace
   [keywords val]
   (assoc keywords :content val))

 (def doc2
  (cx/update-elements-at-path doc [:ScienceKeywords] clear-and-replace update-sk))

 ;; FIND AND replace/clear
 (defn find-and-replace
   [keywords val conditions])
   ;; convert to umm and check for matches
   ;; if matches, set to val

 (cx/element-at-path doc2 [:ScienceKeywords])

 {:science-keywords
  {:umm->xml-function ""
   :xml->umm-function ""}}


 (def sk-find {:Category "CAT5"})

 (def sk {:Category "CAT5"
          :Topic "t1"
          :Term  "term1"})

 (let [ks (select-keys sk (keys sk-find))]
   (= ks sk-find)))
