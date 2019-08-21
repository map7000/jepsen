(defproject jepsen.ignite "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [jepsen "0.1.12"]
                 [javax.cache/cache-api "1.0.0"]
                 [org.apache.ignite/ignite-core "2.8.5-p2"]
                 [org.apache.ignite/ignite-spring "2.8.5-p2"]
                 [org.apache.ignite/ignite-log4j "2.8.5-p2"]
                 [org.gridgain/gridgain-core "8.5.8-p2"]
                 [org.springframework/spring-core "4.3.18.RELEASE"]
                 [org.springframework/spring-beans "4.3.18.RELEASE"]
                 [org.springframework/spring-context "4.3.18.RELEASE"]]
  :java-source-paths ["src/java"]
  :repositories {"local" ~(str (.toURI (java.io.File. "maven_repository")))}
  :target-path "target/%s"
  :main jepsen.ignite.runner
  :aot [jepsen.ignite.runner])
