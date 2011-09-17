(ns neo-lims.core
  (:use [clojure.tools.cli]
        [clj-logging-config.log4j]
        [neo-lims.migrate :only (do-migrate)])
  (:require [borneo.core :as neo]))

(defn- handle-missing-or-invalid-command
  []
  (println "Missing or invalid command")
  (System/exit 1))

(defn- init-logging
  [filename debug verbose]
  ;(set-config-logging-level! :debug)
  (set-logger! "neo-lims"
               :out filename
               :level (cond debug :debug verbose :info :else :warn)
               :pattern "%d %c %m%n"))

(defn migrate
  [args]
  (let [{:keys [db-path log-file limit debug verbose]}
        (cli args
             (required ["--db-path" "Path to create neo4j database"])
             (optional ["--log-file" "Path to write migration log"])
             (optional ["--debug" "Log debug messages" :default false])
             (optional ["--verbose" "Log info messages" :default true])
             (optional ["--limit" "Maximum number of genes to process"] #(Integer. %)))]
    (init-logging log-file debug verbose)
    (neo/with-db! db-path (do-migrate limit))))

(defn -main
  [& args]
  (case (first args)
        "migrate" (migrate (rest args))
        (handle-missing-or-invalid-command)))

    

