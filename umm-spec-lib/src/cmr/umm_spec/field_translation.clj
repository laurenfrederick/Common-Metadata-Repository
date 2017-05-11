(ns cmr.umm-spec.field-translation
  "Contains functions for parsing and generating umm for specific UMM fields"
  (:require
   [clojure.data.xml :as xml]
   [cmr.common.mime-types :as mt]
   [cmr.common.xml :as cx]
   [cmr.common.xml.gen :refer :all]
   [cmr.common.xml.simple-xpath :as xpath]
   [cmr.umm-spec.dif-util :as dif-util]
   [cmr.umm-spec.iso-keywords :as iso-keywords]
   [cmr.umm-spec.json-schema :as js]
   [cmr.umm-spec.migration.version-migration :as vm]
   [cmr.umm-spec.umm-json :as umm-json]
   [cmr.umm-spec.umm-to-xml-mappings.dif10 :as umm-to-dif10]
   [cmr.umm-spec.umm-to-xml-mappings.dif9 :as umm-to-dif9]
   [cmr.umm-spec.umm-to-xml-mappings.echo10 :as umm-to-echo10]
   [cmr.umm-spec.umm-to-xml-mappings.iso-smap :as umm-to-iso-smap]
   [cmr.umm-spec.umm-to-xml-mappings.iso19115-2 :as umm-to-iso19115-2]
   [cmr.umm-spec.umm-to-xml-mappings.serf :as umm-to-serf]
   [cmr.umm-spec.util :as u]
   [cmr.umm-spec.xml-to-umm-mappings.dif10 :as dif10-to-umm]
   [cmr.umm-spec.xml-to-umm-mappings.dif9 :as dif9-to-umm]
   [cmr.umm-spec.xml-to-umm-mappings.echo10 :as echo10-to-umm]
   [cmr.umm-spec.xml-to-umm-mappings.iso-smap :as iso-smap-to-umm]
   [cmr.umm-spec.xml-to-umm-mappings.iso19115-2 :as iso19115-2-to-umm]
   [cmr.umm-spec.xml-to-umm-mappings.serf :as serf-to-umm]))

(defn science-keyword-xml->umm
  "Convert a single science keyword in xml format to umm"
  [xml format])

(defn- generate-iso-keyword
  [keyword]
  [:keyword
   [:CharacterString
    "CAT > TOP1 > TERM > NONE > NONE > NONE > NONE"]])

(defn science-keyword-umm->xml
  "Convert a science keyword to xml for the given format"
  [format science-keyword]
  (xml/parse-str
    (xml
     (condp = format
       :dif (umm-to-dif9/generate-science-keyword science-keyword)
       :dif10 (umm-to-dif10/generate-science-keyword science-keyword)
       :echo10 (umm-to-echo10/generate-science-keyword science-keyword)
       :iso19115 (generate-iso-keyword (iso-keywords/science-keyword->iso-keyword-string science-keyword))
       :iso-smap (generate-iso-keyword (iso-keywords/science-keyword->iso-keyword-string science-keyword))))))
