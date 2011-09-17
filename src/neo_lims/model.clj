(ns neo-lims.model
  (:use [inflections.core :only (plural)])
  (:require [borneo.core :as neo]
            [clojure.string :as str]))

(def *db-path* "/tmp/neo-lims")

(defn- get-subref-node
  [rel-type]
  (when-let [r (neo/single-rel (neo/root) rel-type :out)]
    (neo/end-node r)))

(defn- create-subref-node
  [rel-type name]
  (neo/create-child! rel-type {:name name}))

(defn- create-node
  [type props]
  (let [store-name (plural type)
        subref-rel-type (keyword (str/lower-case store-name))
        node-rel-type (keyword (str/lower-case type))]
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
        (when-let [value (get props attr)]
          (.add index node (name attr) value))))))

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

(defn associate-design-with-gene
  [design gene]
  (neo/create-rel! design :knocks-out gene))

(defn- unqualified-well-name
  [well-name]
  (str/upper-case (subs well-name (- (count well-name) 3))))

(defn- plate-qualified-well-name
  [plate-name well-name]
  (str (str/upper-case plate-name) "_" (unqualified-well-name well-name)))

(defn get-gene-by-marker
  [marker_symbol]
  (get-single-node-via-index "Genes" "marker_symbol" marker_symbol))

(defn get-gene-by-accession
  [mgi_accession_id]
  (get-single-node-via-index "Genes" "mgi_accession_id" mgi_accession_id))

(defn get-design-by-id
  [design_id]
  (get-single-node-via-index "Designs" "design_id" desgin_id))

(defn fetch-genes
  ([]
     (doall (map neo/props (neo/traverse (get-subref-node :genes) :gene))))
  ([wanted]
     (doall (map neo/props (neo/traverse (get-subref-node :genes) :gene)))))

(defn purge-genes
  []
  (neo/with-tx
    (doseq [g (neo/traverse (get-subref-node :genes) :gene)]
      (doseq [r (neo/rels g :gene)]
        (neo/delete! r))
      (neo/delete! g))))

(comment
  (neo/with-db! *db-path*
    (create-gene {:mgi_accession_id "MGI:105369" :marker_symbol "Cbx1"})
    (create-gene {:mgi_accession_id "MGI:1202710" :marker_symbol "Art4"})
    (create-gene {:mgi_accession_id "MGI:1349215" :marker_symbol "Abcd1"})))
