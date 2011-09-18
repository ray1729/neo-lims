(ns neo-lims.migrate
  (:require [clojure.tools.logging :as log]
            [borneo.core :as neo]
            [neo-lims.model :as m]
            [neo-lims.data-helpers :as dh]
            [clojure.contrib.sql :as sql]
            [clojureql.core :as cql]))

(def htgt
     {:classname   "oracle.jdbc.driver.OracleDriver"
      :subprotocol "oracle:oci"
      :subname     "@ESMT"
      :user        "euvect_ro"
      :password    "euvect_ro"
      :fetch-size  1000})

(defn query-genes
  []
  (-> (cql/table :mgi_gene)
      (cql/join (cql/table :project) (cql/where (= :project.mgi_gene_id :mgi_gene.mgi_gene_id)))
      (cql/project [:mgi_gene.mgi_gene_id :mgi_gene.mgi_accession_id :mgi_gene.marker_symbol])
      (cql/distinct)))

(defn query-designs-for-gene
  [mgi_gene_id]
  (-> (cql/join (cql/table :design)
                (cql/table :project)
                (cql/where (= :design.design_id :project.design_id)))
      (cql/select (cql/where (= :project.mgi_gene_id mgi_gene_id)))
      (cql/project [:design.design_id :design.design_type])
      (cql/distinct)))

(defn query-design-wells-for-design
  [design_id]
  (-> (cql/table :project)
      (cql/select (cql/where (= :project.design_id design_id)))
      (cql/join (cql/table :well)
                (cql/where (= :well.design_instance_id :project.design_instance_id)))
      (cql/join (cql/table :plate)
                (cql/where (and (= :plate.plate_id :well.plate_id)
                                (= :plate.type "DESIGN"))))
      (cql/project [[:plate.name :as :plate_name] [:plate.type :as :plate_type]
                    :well.plate_id :well.well_name :well.well_id])
      (cql/distinct)))

(defn query-child-wells-for-well
  [parent_well_id]
  (-> (cql/table :well)
      (cql/select (cql/where (= :well.parent_well_id parent_well_id)))
      (cql/join (cql/table :plate) (cql/where (= :plate.plate_id :well.plate_id)))
      (cql/project [[:plate.name :as :plate_name] [:plate.type :as :plate_type]
                    :well.plate_id :well.well_name :well.well_id])))

(defn get-associated-data
  "Get plate_data (well_data) for plate (well) with plate_id (well_id) id, according to type"
  [type id]
  (let [table-name (keyword (str type "_data"))
        key-name   (keyword (str type "_id"))
        query      (-> (cql/table table-name)
                       (cql/select (cql/where (= key-name id)))
                       (cql/project [:data_type :data_value]))]
    (cql/with-results [data query]
      (zipmap (map #(dh/sanitize-key-name (:data_type %)) data)
              (map :data_value data)))))

(defn get-well-props
  [well]
  (-> (get-associated-data "well" (:well_id well))
      (assoc :well_name (dh/plate-qualified-well-name well))
      (dissoc :well_id)))  

(defn get-plate-props
  [{:keys [plate_name plate_type plate_id]}]
  (-> (get-associated-data "plate" plate_id)
      (assoc :plate_name plate_name :plate_type plate_type)))   

(defn find-or-create-plate
  [{:keys [plate_name] :as plate}]
  (m/with-plate-lock
    (if-let [plate-node (m/get-plate-by-name plate_name)]
      plate-node
      (m/create-plate (get-plate-props plate)))))

(defn migrate-well
  [{:keys [plate_name plate_type well_name] :as well}]
  (log/info (str "Migrating well: " plate_name "[" well_name "]"))  
  (let [plate-node (find-or-create-plate well)
        well-node (m/create-well plate-node (get-well-props well))]
    (cql/with-results [child-wells (query-child-wells-for-well (:well_id well))]
      (doseq [child-well child-wells]
        (let [child-well-node (migrate-well child-well)]
          (m/associate-child-well-with-parent child-well-node well-node))))
    well-node))
  
(defn migrate-design
  [design]
  (log/info (str "Migrating design: " (:design_id design)))
  (let [design-node (m/create-design design)]
    (cql/with-results [design-wells (query-design-wells-for-design (:design_id design))]
      (doseq [well design-wells]
        (let [design-well-node (migrate-well well)]
          (m/associate-well-with-design design-well-node design-node)))
      design-node)))

(defn migrate-gene
  [{:keys [mgi_gene_id] :as gene}]
  (log/info (str "Migrating gene: " mgi_gene_id))
  (let [gene-node (m/create-gene gene)]
    (cql/with-results [designs (query-designs-for-gene mgi_gene_id)]
      (doseq [design designs]
        (let [design-node (migrate-design design)]
          (m/associate-design-with-gene design-node gene-node))))
    gene-node))

(defn do-migrate
  [lim]
  (log/info (str "Migrating " (if (> lim 0) lim "all") " genes"))
  (sql/with-connection htgt
    (cql/with-results [genes (query-genes)]
      (doseq [gene (if (> lim 0) (take lim genes) genes)]
        (neo/with-tx (migrate-gene gene))))))
