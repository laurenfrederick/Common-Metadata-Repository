(ns cmr.umm-spec.native-field-update
  "Contains functions for updating a field in place in its native format"
  (:require
   [clojure.data.xml :as xml] ;;;;
   [clojure.java.io :as io] ;;;;
   [cmr.common.xml :as cx]
   [cmr.umm-spec.field-translation :as field-translation]
   [cmr.umm-spec.test.location-keywords-helper :as lkt] ;;;;))
   [cmr.umm-spec.umm-spec-core :as core] ;;;;
   [cmr.common.xml.gen :refer :all]))

(def test-context (lkt/setup-context-for-test))

(defn resolve-xml
  [xml1 xml2 path]
  (def xml1 xml1)
  (let [tag (first path)
        rest-path (rest path)
        atom-xml2-index (atom 0)
        atom-found-element (atom false)
        orig-has-elements (some? (seq (cx/elements-at-path xml1 path)))]
    (for [index (range (count (:content xml1)))
          :let [xml1-content (nth (:content xml1) index)
                xml2-content (nth (:content xml2) @atom-xml2-index)
                _ (println (:tag xml1-content) " " (:tag xml2-content))]
          :when (< @atom-xml2-index (count (:content xml2)))
          :let [_ (reset! atom-found-element (or @atom-found-element (= tag (:tag xml1-content))))
                _ (println @atom-xml2-index " " @atom-found-element)]]
      (cond
        (= (:tag xml1-content) (:tag xml2-content))
        (do
          (swap! atom-xml2-index inc)
          (if (= tag (:tag xml1-content))
            xml2-content
            xml1-content))
        ;; Not equal tags
        (= tag (:tag xml1-content))
        (do
          (swap! atom-xml2-index inc)
          nil)
        (= tag (:tag xml2-content))
        (if (or (not orig-has-elements) ; Adding to xml1\
                @atom-found-element) ; Adding an extra
          (do
            (swap! atom-xml2-index inc)
            [xml2-content xml1-content])
          xml1-content) ; Wait until we get to
        :else xml1-content))))

(def xml1
 {:content [{:tag :A :content ["A"]}
            {:tag :B :content ["B"]}
            {:tag :C :content ["C"]}
            {:tag :Params :content ["P1"]}
            {:tag :Params :content ["P2"]}
            {:tag :E :content ["E"]}]})

(def xml2
 {:content [{:tag :A :content ["A"]}
            {:tag :C :content ["C"]}
            {:tag :Params :content ["P1"]}
            {:tag :Params :content ["P2"]}
            {:tag :Params :content ["P3"]}
            {:tag :E :content ["E"]}]})

(defn- iso-is-science-keyword
  [element]
  (= "theme"
     (:codeListValue (:attrs (cx/element-at-path element [:type :MD_KeywordTypeCode])))))

(defmulti field-configuration
 (fn [format]
   format))

(defmethod field-configuration :echo10
  [format]
  {:science-keyword {:umm-to-xml-function field-translation/science-keyword-umm->xml
                     :xml-location [:ScienceKeywords]}})

(defmethod field-configuration :iso19115
  [format]
  {:science-keyword {:umm-to-xml-function field-translation/science-keyword-umm->xml
                     :xml-location [:identificationInfo
                                    :MD_DataIdentification
                                    :descriptiveKeywords
                                    :MD_Keywords]
                     :xml-individual-location [:keywords]
                     :perform-update-fn iso-is-science-keyword}})

(defn- perform-update?
  [perform-update-fn element]
  (if (nil? perform-update-fn)
    true
    (perform-update-fn element)))

(defn- get-configuration
  [format field-key configuration-key]
  (-> (field-configuration format)
      (get field-key)
      (get configuration-key)))

(defmulti update-xml
  (fn [element update-type update-value perform-update-fn find-value]
    update-type))

; (defmethod :find-and-remove
;   [element update-type update-value perform-update-fn find-value])

(defmethod update-xml :add-to-existing
  [element update-type update-value perform-update-fn find-value]
  (if (perform-update? perform-update-fn element)
    (update element :content #(conj % update-value))
    element))

(defmethod update-xml :clear-all-and-replace
  [element update-type update-value perform-update-fn find-value]
  (if (perform-update? perform-update-fn element)
    (let [additional-elements (filter #(not= (:tag update-value) (:tag %)) (:content element))]
      (assoc element :content (conj additional-elements update-value)))
    element))

(defn update-field
  "Update a field in the metadata."
  [metadata format field update-type update-value find-value]
  (let [umm-to-xml-function (get-configuration format field :umm-to-xml-function)
        update-value-xml (umm-to-xml-function format update-value)
        update-path (get-configuration format field :xml-location)
        perform-update-fn (get-configuration format field :perform-update-fn)]
    (cx/update-elements-at-path metadata update-path update-xml update-type update-value-xml perform-update-fn find-value)))

(defmulti update-umm
  (fn [update-type umm field-key update-value]
    update-type))

; (defmethod :find-and-remove
;   [element update-type update-value perform-update-fn find-value])

(defmethod update-umm :add-to-existing
  [update-type umm field-key update-value]
  (update umm field-key #(conj % update-value)))

(defmethod update-umm :clear-all-and-replace
  [update-type umm field-key update-value]
  (assoc umm field-key [update-value]))

(defn copy-elements
  [element]
  (def element element))

(defn update-field-umm
  [context metadata format field update-type update-value find-value]
  (def metadata metadata)
  (let [umm (core/parse-metadata test-context :collection format metadata)
        updated-umm (update-umm update-type umm field update-value)
        orig-metadata (xml/parse-str metadata)
        updated-metadata (xml/parse-str (core/generate-metadata context updated-umm format))
        updated-keys (cx/elements-at-path updated-metadata [:Parameters])
        resolved-metadata (-> orig-metadata
                             (assoc :content (resolve-xml orig-metadata updated-metadata [:Parameters]))
                             (dissoc :attrs))]
    (def resolved-metadata resolved-metadata)))
    ;(core/validate-metadata :collection format (xml/emit-str resolved-metadata))))

(def sk
  {:Category "CAT"
   :Topic "TOP1"
   :Term "TERM"})

;; Add to existing
;; Clear all and replace
;; Find and remove
;; Find and replace

(comment

  (def md
   (str "<DIF xmlns=\"http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/\" xmlns:dif=\"http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/ http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/dif_v9.9.3.xsd\">"
    (cx/remove-xml-processing-instructions (xml/emit-str (:content resolved-metadata)))
    "</DIF>"))

  (core/validate-metadata :collection :dif md)


  (let [metadata (slurp (io/resource "example_data/dif/C1214305813-AU_AADC.xml"))]
    (update-field-umm test-context metadata :dif :ScienceKeywords :add-to-existing sk nil))
    ;(def xml1 (xml/parse-str metadata)))
  (core/validate-metadata :collection :dif (slurp (io/resource "example_data/dif/C1214305813-AU_AADC.xml")))

  (def field :science-keyword)
  (def format :echo10)

  (def xml
    (field-translation/science-keyword-umm->xml :iso19115 sk))

  (update-field nil :echo10 :science-keyword nil sk nil)

  (def metadata
   ;(xml/parse (java.io.StringReader. (slurp (io/resource "example_data/echo10/C179001707-SEDAC.xml"))))
   (xml/parse-str (slurp (io/resource "example_data/iso19115/artificial_test_data_2.xml"))))



  (let [data
        (update-field metadata :iso19115 :science-keyword :clear-all-and-replace sk nil)]
   (cx/elements-at-path metadata [:identificationInfo
                                  :MD_DataIdentification
                                  :descriptiveKeywords
                                  :MD_Keywords]))

  (let [metadata (xml/parse (java.io.StringReader. (slurp (io/resource "example_data/echo10/C179001707-SEDAC.xml"))))
        data (update-field metadata :echo10 :science-keyword :clear-all-and-replace sk nil)]
    (cx/elements-at-path data [:ScienceKeywords]))


  (def elements
    (cx/elements-at-path metadata [:identificationInfo
                                   :MD_DataIdentification
                                   :descriptiveKeywords
                                   :MD_Keywords]))

  (def e (second elements))
  (:codeListValue (:attrs (cx/element-at-path e [:type :MD_KeywordTypeCode]))))
