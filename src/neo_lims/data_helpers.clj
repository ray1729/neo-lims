(ns neo-lims.data-helpers
  (:require [clojure.string :as str]))

(defn unqualified-well-name
  [well-name]
  (str/upper-case (subs well-name (- (count well-name) 3))))

(defn plate-qualified-well-name
  [{:keys [plate_name well_name]}]
  (str (str/upper-case plate_name) "_" (unqualified-well-name well_name)))

(defn sanitize-key-name
  [k]
  (-> k
      str/trim
      (str/replace #"\s+" "_")
      (str/replace #"\W" "")
      str/lower-case
      keyword))

