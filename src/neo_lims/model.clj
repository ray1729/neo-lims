(ns neo-lims.model
  (:use [inflections.core :only (plural)])
  (:require [borneo.core :as neo]
            [clojure.string :as str]))

(defn- get-subref-node
  [rel-type]
  (when-let [r (neo/single-rel (neo/root) rel-type :out)]
    (neo/end-node r)))

(defn- create-subref-node
  [rel-type name]
  (neo/create-child! rel-type {:name name}))

(defn- sanitize-prop-val
  "Ensure that the property value v is suitable for a neo4j properties container. At
   the moment, just casts BigInteger to long, but could do more."
  [v]
  (if (= (class v) (class 1M)) (long v) v))

(defn- create-node
  [type props]
  (let [store-name (plural type)
        subref-rel-type (keyword (str/lower-case store-name))
        node-rel-type (keyword (str/lower-case type))
        props (zipmap (keys props) (map sanitize-prop-val (vals props)))]
    (neo/with-tx
      (let [subref-node (or (get-subref-node subref-rel-type)
                             (create-subref-node subref-rel-type store-name))]
        (neo/create-child! subref-node node-rel-type props)))))

(defn- get-node-index
  [name]
  (.forNodes (neo/index) name))

(defn- index-node-attrs
  [index-name node props & attrs]
  (when (seq attrs)
    (let [index (get-node-index index-name)]
      (doseq [attr attrs]
        (let [value (get props attr)]
          (when (not (nil? value)) (.add index node (name attr) value)))))))

(defn- get-single-node-via-index
  [index-name key value]
  (.getSingle (.get (get-node-index index-name) key value)))

(defn create-gene
  [gene]
  (neo/with-tx
    (let [gene-node (create-node "Gene" gene)]
      (index-node-attrs "Genes" gene-node gene :mgi_accession_id :marker_symbol)
      gene-node)))

(defn create-design
  [design]
  (neo/with-tx
    (let [design-node (create-node "Design" design)]
      (index-node-attrs "Designs" design-node design :design_id)
      design-node)))

(defn- unqualified-well-name
  [well-name]
  (str/upper-case (subs well-name (- (count well-name) 3))))

(defn- plate-qualified-well-name
  [{:keys [plate_name well_name]}]
  (str (str/upper-case plate_name) "_" (unqualified-well-name well_name)))

(defn create-well
  [well]
  (neo/with-tx
    (let [well (assoc well :well_name (plate-qualified-well-name well))
          well-node (create-node "Well" well)]
      (index-node-attrs "Wells" well-node well :plate_name :well_name)
      well-node)))

(defn associate-design-with-gene
  [design gene]
  (neo/create-rel! design :knocks-out gene))

(defn associate-well-with-design
  [well design]
  (neo/create-rel! well :instance-of-design design))

(defn associate-child-well-with-parent
  [child-well parent-well]
  (neo/create-rel! child-well :child-of parent-well))

(defn get-gene-by-marker
  [marker_symbol]
  (get-single-node-via-index "Genes" "marker_symbol" marker_symbol))

(defn get-gene-by-accession
  [mgi_accession_id]
  (get-single-node-via-index "Genes" "mgi_accession_id" mgi_accession_id))

(defn get-designs-for-marker
  [marker_symbol]
  (when-let [g (get-gene-by-marker marker_symbol)]
    (map neo/start-node (neo/rels g :knocks-out :in))))

(defn get-design-by-id
  [design_id]
  (get-single-node-via-index "Designs" "design_id" design_id))

(defn get-well-by-name
  [well_name]
  (get-single-node-via-index "Wells" "well_name" well_name))

(defn get-ancestors-of-well
  [well_name]
  (when-let [well-node (get-well-by-name well_name)]
    (neo/traverse well-node :child-of)))

(defn get-design-node-from-well-node
  [well-node]
  (when-let [ancestors (neo/traverse well-node :child-of)]
    (when-let [design-rel (neo/single-rel (last ancestors) :instance-of-design)]
      (neo/end-node design-rel))))

(defn get-gene-node-from-well-node
  [well-node]
  (when-let [design-node (get-design-node-from-well-node well-node)]
    (when-let [gene-rel (neo/single-rel design-node :knocks-out)]
      (neo/end-node gene-rel))))

(defn fetch-genes
  ([]
     (doall (map neo/props (neo/traverse (get-subref-node :genes) :gene))))
  ([wanted]
     (doall (map neo/props (neo/traverse (get-subref-node :genes) :gene)))))

;; (defn purge-genes
;;   []
;;   (neo/with-tx
;;     (doseq [g (neo/traverse (get-subref-node :genes) :gene)]
;;       (doseq [r (neo/rels g :gene)]
;;         (neo/delete! r))
;;       (neo/delete! g))))
