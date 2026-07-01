(ns fleet.run
  "Demo: set up a fleet Datom log, have agents lease work + submit proposals, and
  drive the FleetCoordinatorActor through coordination ticks that exercise the
  invariant — accept (lease-held), hold (non-holder), sign-off (protected path).

  Run: clojure -M:dev:run"
  (:require [langgraph.graph :as g]
            [fleet.actor :as actor]
            [kotoba.fleet.governor :as kgov]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.store :as store]))

(defn- tick [a db tid now]
  (let [r (g/run* a {:db db} {:thread-id tid :now now})]
    (if (= :interrupted (:status r))
      (do (println "  ⏸  interrupted → human sign-off; resuming…")
          (g/run* a nil {:thread-id tid}))
      r)))

(defn -main [& _]
  (let [db (store/mem-store)
        wrote (atom [])
        a  (actor/build {:now 100 :materialize #(swap! wrote conj (:work %))})]
    ;; agent A holds src/foo.clj and proposes a write; B proposes on src/bar.clj
    ;; without holding it; A proposes a protected manifest write.
    (lease/claim! db {:work "src/foo.clj" :agent "A" :ttl-ms 100000 :now 0})
    (lease/claim! db {:work "manifest/west.yml" :agent "A" :ttl-ms 100000 :now 0})
    (kgov/submit-proposal! db {:work "src/foo.clj" :agent "A" :payload {:edit 1} :now 1})
    (kgov/submit-proposal! db {:work "src/bar.clj" :agent "B" :payload {:edit 2} :now 2})
    (kgov/submit-proposal! db {:work "manifest/west.yml" :agent "A" :payload {:edit 3} :now 3})
    (doseq [n [1 2 3]]
      (let [r (tick a db (str "tick-" n) 100)
            last-audit (last (get-in r [:state :audit]))]
        (println (str "tick " n " → " (:t last-audit)))))
    (println "materialized:" @wrote)))
