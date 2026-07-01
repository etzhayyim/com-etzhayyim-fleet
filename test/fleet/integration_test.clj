(ns fleet.integration-test
  "End-to-end: the whole fleet stack (agent → runner → coordinator ⊣ FleetGovernor
  → view) upholds ADR-2606302000's thesis — many agents share one repo, and every
  git write is governed. This is the capstone regression for `fleet.sim`."
  (:require [clojure.test :refer [deftest is testing]]
            [fleet.sim :as sim]
            [kotoba.fleet.view :as view]))

(deftest many-agents-one-repo-no-conflict
  (let [{:keys [materialized held racer]} (sim/simulate {})]
    (testing "5 disjoint work-units all materialize — parallel, no git conflict"
      (is (= ["src/a.clj" "src/b.clj" "src/c.clj" "src/d.clj" "src/e.clj"]
             (filterv #(re-find #"^src/[a-e]\.clj$" %) materialized))))
    (testing "each materialized work-unit was written exactly once (no double-write)"
      (is (apply distinct? materialized)))
    (testing "the protected path materializes too — but only after human sign-off"
      (is (some #{"manifest/west.yml"} materialized)))
    (testing "an expired-lease proposal is HELD by the govern-time invariant, never materializes"
      (is (some #{"src/stale.clj"} held))
      (is (not (some #{"src/stale.clj"} materialized))))
    (testing "the racer lost the optimistic claim and never ran its session"
      (is (= :contended racer)))
    (testing "exactly the 6 legit units reached git (5 src + 1 protected)"
      (is (= 6 (count materialized))))))

(deftest snapshot-reflects-final-state
  (testing "every proposal was governed → the fleet view shows nothing pending"
    (let [{:keys [db now]} (sim/simulate {})]
      (is (zero? (:pending (view/snapshot db now)))))))
