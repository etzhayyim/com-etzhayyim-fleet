(ns fleet.node-test
  "The launcher's role dispatch (fleet.node/run!) drives the node loop over a
  store — the same core -main uses, exercised without env/stdout."
  (:require [clojure.test :refer [deftest is testing]]
            [fleet.node :as node]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.store :as store]))

(defn- run [unit] {unit (str "// " unit)})

(deftest role-agent-only-proposes
  (testing ":agent role claims + proposes open work but never materializes"
    (let [db (store/mem-store)]
      (agent/enqueue! db {:unit "src/a.clj" :created-by "root"})
      (let [r (node/run-role! db {:role :agent :agents ["a1"] :run run :now 1000})]
        (is (= 1 (:proposed r)))
        (is (seq (gov/pending-proposals db)) "proposal is queued, not yet materialized")))))

(deftest role-governor-only-materializes
  (testing ":governor role drains pending proposals (single writer)"
    (let [db (store/mem-store)]
      (agent/enqueue! db {:unit "src/a.clj" :created-by "root"})
      (node/run-role! db {:role :agent :agents ["a1"] :run run :now 1000})
      (let [r (node/run-role! db {:role :governor :now 1000})]
        (is (= ["src/a.clj"] (:materialized r)))
        (is (zero? (:pending r)))))))

(deftest role-both-drains-queue
  (testing ":both role runs the full durable loop"
    (let [db (store/mem-store)]
      (doseq [u ["src/a.clj" "src/b.clj"]] (agent/enqueue! db {:unit u :created-by "root"}))
      (let [r (node/run-role! db {:role :both :agents ["a1" "a2"] :run run :budget 5})]
        (is (= #{"src/a.clj" "src/b.clj"} (set (:materialized r))))
        (is (zero? (:remaining r)))))))

(deftest agent-then-separate-governor-node
  (testing "agent node + governor node (separate run! calls, shared store) = F4 split"
    (let [db (store/mem-store)]
      (doseq [u ["src/a.clj" "src/b.clj" "src/c.clj"]] (agent/enqueue! db {:unit u :created-by "root"}))
      (node/run-role! db {:role :agent :agents ["pc1-a1"] :run run :now 1000})
      (node/run-role! db {:role :agent :agents ["pc2-a1"] :run run :now 1000})
      (let [r (node/run-role! db {:role :governor :now 1000})]
        (is (= 3 (count (:materialized r))) "all units materialized once by the single governor")
        (is (apply distinct? (:materialized r)))))))
