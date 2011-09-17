(defproject neo-lims "1.0.0-SNAPSHOT"
  :description "Prototype LIMS with neo4j graph database backend"
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [borneo "0.2.0"]
                 [inflections "0.5.3-SNAPSHOT"]
                 [clojureql "1.1.0-SNAPSHOT"]
                 [ojdbc/ojdbc "6"]
                 [log4j/log4j "1.2.16"]
                 [org.clojure/tools.logging "0.1.2"]
                 [clj-logging-config "1.7.0"]
                 [org.clojure/tools.cli "0.1.0"]]
  :main neo-lims.core)
