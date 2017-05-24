(ns cmr.umm-spec.field-update
 "Functions to apply an update of a particular type to a field-translation"
 (:require
  [cmr.common.util :as util]
  [cmr.umm-spec.umm-spec-core :as spec-core]))

(defn field-update-functions
  "Partial update functions for handling specific update cases"
  [umm]
  {[:Instruments] (partial util/update-in-each umm [:Platforms])})

(defn- value-matches?
  "Return true if the value is a match on find value."
  [find-value value]
  (= (select-keys value (keys find-value))
     find-value))

(defmulti apply-umm-list-update
  "Apply the umm update by type. Assumes that the update-field is a list in
  the umm collection. Update-field should be a vector to handle nested fields."
  (fn [update-type umm update-field update-value find-value]
    update-type))

(defmethod apply-umm-list-update :add-to-existing
  [update-type umm update-field update-value find-value]
  (update-in umm update-field #(conj % update-value)))

(defmethod apply-umm-list-update :clear-all-and-replace
  [update-type umm update-field update-value find-value]
  (assoc-in umm update-field [update-value]))

(defmethod apply-umm-list-update :find-and-remove
  [update-type umm update-field update-value find-value]
  (if (seq (get-in umm update-field))
    (update-in umm update-field #(remove (fn [x]
                                           (value-matches? find-value x))
                                         %))
    umm))

(defmethod apply-umm-list-update :find-and-replace
  [update-type umm update-field update-value find-value]
  (if (seq (get-in umm update-field))
    (let [update-value (util/remove-nil-keys update-value)]
      ;; For each entry in update-field, if we find it using the find params,
      ;; update only the fields supplied in update-value with nils removed
      (update-in umm update-field #(map (fn [x]
                                          (if (value-matches? find-value x)
                                            (merge x update-value)
                                            x))
                                        %)))
    umm))

(defn apply-update
  "Apply an update to a umm record. Check for field-specific function and use that
  if exists, otherwise apply the default list update."
  [update-type umm update-field update-value find-value]
  (if-let [partial-update-fn (get (field-update-functions umm) update-field)]
    (partial-update-fn #(apply-umm-list-update update-type % update-field update-value find-value))
    (apply-umm-list-update update-type umm update-field update-value find-value)))

(defn update-concept
  "Apply an update to a raw concept. Convert to UMM, apply the update, and
  convert back to native format."
  [context concept update-type update-field update-value find-value]
  (let [{:keys [format metadata concept-type]} concept
        umm (spec-core/parse-metadata context concept-type format metadata {:sanitize? false})
        umm (apply-update update-type umm update-field update-value find-value)]
    (spec-core/generate-metadata context umm (:format concept))))