(ns fleet.driver-test
  "The durable outer loop: a bounded, crash-recoverable node that drains a work
  queue through the fleet, and recovers a crashed agent's work via lease TTL —
  no bespoke recovery code, just the append-only log."
  (:require [clojure.test :refer [deftest is testing]]
            [fleet.driver :as driver]
            [kotoba.fleet.agent :as agent]
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

(deftest budget-bounds-the-loop
  (testing "the loop never exceeds its round budget"
    (let [db (store/mem-store)]
      ;; a run fn that always yields work but on a unit that stays contended-free;
      ;; here we just assert budget caps rounds even with steady input
      (doseq [u (map #(str "src/" % ".clj") (range 3))]
        (agent/enqueue! db {:unit u :created-by "root"}))
      (let [{:keys [rounds]} (driver/run-node! db {:agents ["a1"] :run edit-run :budget 2})]
        (is (<= rounds 2) "rounds bounded by budget")))))
