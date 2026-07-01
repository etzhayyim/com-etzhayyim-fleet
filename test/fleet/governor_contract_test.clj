(ns fleet.governor-contract-test
  "The fleet invariant: the FleetCoordinatorActor NEVER materializes a write the
  FleetGovernor would reject. A write reaches git ONLY IF the proposing agent
  holds the lease, the gate passes, and (for protected paths) a human signs off."
  (:require [clojure.test :refer [deftest is testing]]
            [langgraph.graph :as g]
            [fleet.actor :as actor]
            [fleet.governor :as gov]
            [kotoba.fleet.governor :as kgov]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.store :as store]))

(defn- fresh []
  (let [db    (store/mem-store)
        wrote (atom [])
        a     (actor/build {:now 100 :materialize #(swap! wrote conj (:work %))})]
    {:db db :wrote wrote :actor a}))

(defn- run-tick [a db tid & [{:keys [resume?]}]]
  (let [r (g/run* a {:db db} {:thread-id tid :now 100})]
    (if (and resume? (= :interrupted (:status r)))
      (g/run* a nil {:thread-id tid})
      r)))

(deftest lease-holder-materializes
  (testing "holder + gate-pass + safe path → materialized"
    (let [{:keys [db wrote actor]} (fresh)]
      (lease/claim! db {:work "src/foo.clj" :agent "A" :ttl-ms 99999 :now 0})
      (kgov/submit-proposal! db {:work "src/foo.clj" :agent "A" :payload 1 :now 1})
      (let [r (run-tick actor db "t1")]
        (is (= :materialize (get-in r [:state :decision :kind])))
        (is (= ["src/foo.clj"] @wrote) "the write reached git")))))

(deftest non-holder-is-held
  (testing "a proposal from an agent that does NOT hold the lease never materializes"
    (let [{:keys [db wrote actor]} (fresh)]
      ;; A holds src/bar.clj; B proposes on it without a lease
      (lease/claim! db {:work "src/bar.clj" :agent "A" :ttl-ms 99999 :now 0})
      (kgov/submit-proposal! db {:work "src/bar.clj" :agent "B" :payload 1 :now 1})
      (let [r (run-tick actor db "t2")]
        (is (= :hold (get-in r [:state :decision :kind])))
        (is (empty? @wrote) "non-holder write blocked before git")
        (is (some #(= :govern-hold (:t %)) (get-in r [:state :audit])))))))

(deftest gate-rejection-is-held
  (testing "a gate-rejected proposal never materializes even from the holder"
    (let [db    (store/mem-store)
          wrote (atom [])
          a     (actor/build {:now 100 :gate (constantly false)
                              :materialize #(swap! wrote conj (:work %))})]
      (lease/claim! db {:work "src/foo.clj" :agent "A" :ttl-ms 99999 :now 0})
      (kgov/submit-proposal! db {:work "src/foo.clj" :agent "A" :payload 1 :now 1})
      (let [r (run-tick a db "t3")]
        (is (= :hold (get-in r [:state :decision :kind])))
        (is (empty? @wrote))))))

(deftest protected-path-requires-signoff
  (testing "a protected-path write pauses for human sign-off, not auto-materialize"
    (let [{:keys [db wrote actor]} (fresh)]
      (lease/claim! db {:work "manifest/west.yml" :agent "A" :ttl-ms 99999 :now 0})
      (kgov/submit-proposal! db {:work "manifest/west.yml" :agent "A" :payload 1 :now 1})
      (let [paused (g/run* actor {:db db} {:thread-id "t4" :now 100})]
        (is (= :interrupted (:status paused)) "paused before human-signoff")
        (is (empty? @wrote) "nothing materialized while awaiting sign-off")
        (let [resumed (g/run* actor nil {:thread-id "t4"})]
          (is (= ["manifest/west.yml"] @wrote) "materialized only after sign-off")
          (is (some #(= :human-signoff (:t %)) (get-in resumed [:state :audit]))))))))

(deftest expired-lease-blocks-materialize
  (testing "once a lease expires the holder loses the right to materialize"
    (let [{:keys [db wrote actor]} (fresh)]
      (lease/claim! db {:work "src/foo.clj" :agent "A" :ttl-ms 10 :now 0}) ; expires by now=100
      (kgov/submit-proposal! db {:work "src/foo.clj" :agent "A" :payload 1 :now 1})
      (let [r (run-tick actor db "t5")]
        (is (= :hold (get-in r [:state :decision :kind])) "expired lease → held")
        (is (empty? @wrote))))))

;; direct unit check on the governor, independent of the graph
(deftest governor-direct
  (let [db (store/mem-store)]
    (lease/claim! db {:work "w" :agent "A" :ttl-ms 999 :now 0})
    (lease/claim! db {:work "manifest/x" :agent "A" :ttl-ms 999 :now 0})
    (is (:ok? (gov/check db {:work "w" :agent "A"} {:now 1})))
    (is (not (:ok? (gov/check db {:work "w" :agent "B"} {:now 1}))) "non-holder rejected")
    (is (:needs-signoff (gov/check db {:work "manifest/x" :agent "A"} {:now 1}))
        "holder on a protected path needs sign-off")))
