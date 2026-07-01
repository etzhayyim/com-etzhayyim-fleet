(ns fleet.driver-test
  "The durable outer loop: a bounded, crash-recoverable node that drains a work
  queue through the fleet, and recovers a crashed agent's work via lease TTL —
  no bespoke recovery code, just the append-only log."
  (:require [clojure.test :refer [deftest is testing]]
            [fleet.driver :as driver]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.store :as store]))

(defn- edit-run [unit] {unit (str "// edited " unit)})

(deftest drains-the-work-queue
  (testing "a node drains every open work-unit and closes it (nothing remains)"
    (let [db (store/mem-store)]
      (doseq [u ["src/a.clj" "src/b.clj" "src/c.clj"]]
        (agent/enqueue! db {:unit u :created-by "root"}))
      (let [{:keys [materialized remaining]}
            (driver/run-node! db {:agents ["a1" "a2"] :run edit-run :budget 10})]
        (is (= #{"src/a.clj" "src/b.clj" "src/c.clj"} (set materialized)) "all units materialized")
        (is (= 3 (count materialized)) "each materialized exactly once")
        (is (zero? remaining) "no open work left — all closed")))))

(deftest idempotent-no-rework
  (testing "re-running the node after a full drain does nothing (done work not re-picked)"
    (let [db (store/mem-store)]
      (agent/enqueue! db {:unit "src/a.clj" :created-by "root"})
      (driver/run-node! db {:agents ["a1"] :run edit-run :budget 10})
      (let [{:keys [materialized rounds]}
            (driver/run-node! db {:agents ["a1"] :run edit-run :budget 10})]
        (is (empty? materialized) "nothing re-materialized")
        (is (zero? rounds) "loop exits immediately — no open work, no pending")))))

(deftest crash-recovery-via-lease-ttl
  (testing "a crashed agent's held lease expires; another agent completes the work"
    (let [db (store/mem-store)]
      (agent/enqueue! db {:unit "src/a.clj" :created-by "root"})
      ;; a crashed agent: holds a short lease, never proposes, never releases
      (lease/claim! db {:work "src/a.clj" :agent "crashed" :ttl-ms 10 :now 0})
      (is (= "crashed" (lease/holder db "src/a.clj" 5)) "crashed agent holds it briefly")
      ;; the driver starts later (now-fn ≥ 1000 > TTL 10): the lease is expired,
      ;; so the unit is open again and a healthy agent completes it
      (let [{:keys [materialized]}
            (driver/run-node! db {:agents ["healthy"] :run edit-run :budget 10})]
        (is (= ["src/a.clj"] materialized) "work recovered and materialized after the crash")))))

;; ── F4: multi-node roles over one shared Datom graph ──

(deftest two-agent-nodes-one-governor-no-double-work
  (testing "agents on 2 nodes share one graph; a single governor materializes each unit once"
    (let [db (store/mem-store)]                       ; the shared kotoba-db graph (in-mem stand-in)
      (doseq [u ["src/a.clj" "src/b.clj" "src/c.clj" "src/d.clj"]]
        (agent/enqueue! db {:unit u :created-by "root"}))
      ;; PC1 and PC2 each run an agent round against the SAME store
      (driver/agent-round! db {:agents ["pc1-a1" "pc1-a2"] :run edit-run :now 1000})
      (driver/agent-round! db {:agents ["pc2-a1" "pc2-a2"] :run edit-run :now 1000})
      ;; exactly ONE node runs the governor (single git writer)
      (let [done (driver/govern! db {:now 1000})]
        (is (= #{"src/a.clj" "src/b.clj" "src/c.clj" "src/d.clj"} (set done)) "all units materialized")
        (is (apply distinct? done) "each unit materialized exactly once across both nodes")
        (is (empty? (gov/pending-proposals db)) "queue fully drained")))))

(deftest cross-node-lease-exclusion
  (testing "two nodes racing for the same unit — one proposes, the other backs off"
    (let [db (store/mem-store)]
      (agent/enqueue! db {:unit "src/hot.clj" :created-by "root"})
      (let [r1 (agent/claim-and-propose! db {:unit "src/hot.clj" :agent "pc1-a1" :ttl-ms 999 :now 1000 :run edit-run})
            r2 (agent/claim-and-propose! db {:unit "src/hot.clj" :agent "pc2-a1" :ttl-ms 999 :now 1000 :run edit-run})]
        (is (= :proposed (:status r1)) "PC1 won the optimistic claim")
        (is (= :contended (:status r2)) "PC2 lost — no lock server, no double-propose")
        (let [done (driver/govern! db {:now 1000})]
          (is (= ["src/hot.clj"] done) "the hot unit is materialized exactly once"))))))

(deftest budget-bounds-the-loop
  (testing "the loop never exceeds its round budget"
    (let [db (store/mem-store)]
      ;; a run fn that always yields work but on a unit that stays contended-free;
      ;; here we just assert budget caps rounds even with steady input
      (doseq [u (map #(str "src/" % ".clj") (range 3))]
        (agent/enqueue! db {:unit u :created-by "root"}))
      (let [{:keys [rounds]} (driver/run-node! db {:agents ["a1"] :run edit-run :budget 2})]
        (is (<= rounds 2) "rounds bounded by budget")))))
