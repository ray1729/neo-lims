(ns neo-lims.migrate
  (:require [clojure.tools.logging :as log]
            [borneo.core :as neo]
            [neo-lims.model :as m]           
            [clojure.contrib.sql :as sql]
            [clojureql.core :as cql]))

(def htgt
     {:classname   "oracle.jdbc.driver.OracleDriver"
      :subprotocol "oracle:oci"
      :subname     "@ESMP"
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
      (cql/project [[:plate.name :as :plate_name] :well.well_name :well.well_id])
      (cql/distinct)))

(defn query-well-data-for-well
  [well_id]
  (-> (cql/table :well_data)
      (cql/select (cql/where (= :well_id well_id)))
      (cql/project [:data_type :data_value])))

(defn query-child-wells-for-well
  [parent_well_id]
  (-> (cql/table :well)
      (cql/select (cql/where (= :well.parent_well_id parent_well_id)))
      (cql/join (cql/table :plate) (cql/where (= :plate.plate_id :well.plate_id)))
      (cql/project [[:plate.name :as :plate_name] :well.well_name :well.well_id])))

(defn migrate-well
  [{:keys [plate_name well_name] :as well}]
  (log/info (str "Migrating well: " plate_name "[" well_name "]"))
  (let [well-node (m/create-well well)]    
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
  [& [lim]]
  (log/info (str "Migrating " (if lim lim "all") " genes"))
  (sql/with-connection htgt
    (cql/with-results [genes (query-genes)]
      (doseq [gene (if lim (take lim genes) genes)]
        (neo/with-tx (migrate-gene gene))))))
