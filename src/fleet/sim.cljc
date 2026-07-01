(ns fleet.sim
  "Capstone: many agents coordinate on ONE repo through the fleet — no git
  conflict. This wires every piece of the stack end-to-end (kotoba-fleet agent +
  fleet.runner capturing host + this actor's coordinator ⊣ FleetGovernor +
  kotoba-fleet view), demonstrating ADR-2606302000's thesis WITHOUT external
  creds (mock coding sessions, in-memory store).

  Scenario (7 work-units, 8 agents across 2 PCs):
    - 5 agents lease 5 distinct src files, run a (mock) coding session that
      captures a write, and propose         → all 5 materialize (parallel, no conflict)
    - 1 agent leases a PROTECTED path (manifest/) and proposes
                                             → governor pauses for human sign-off, then materializes
    - 1 agent leases with a short TTL that EXPIRES before the governor drains
                                             → govern-time lease invariant HOLDS it (never materializes)
    - 1 racer contends for an already-held unit
                                             → loses the optimistic claim, never even runs (lock-free exclusion)

  One governed coordinator is the only writer to git; agents only ever append
  leases + proposals. Run: clojure -M:dev:run"
  (:require [langgraph.graph :as g]
            [fleet.actor :as actor]
            [fleet.runner :as runner]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.view :as view]
            [kotoba.fleet.store :as store]))

(def work
  "[agent unit ttl-ms] — pc1-*/pc2-* = two PCs. stale has a TTL that expires by
  drain time; manifest/* is a protected path."
  [["pc1-a1" "src/a.clj"        100000]
   ["pc1-a2" "src/b.clj"        100000]
   ["pc2-a3" "src/c.clj"        100000]
   ["pc2-a4" "src/d.clj"        100000]
   ["pc1-a5" "src/e.clj"        100000]
   ["pc2-a6" "manifest/west.yml" 100000]   ; protected → sign-off
   ["pc1-a7" "src/stale.clj"        10]])  ; TTL 10, drained at now=1000 → expired

(defn- coding-run-for
  "A mock coding session for `agent`: writes one file (its ns stub) and proposes."
  [agent]
  (runner/coding-run
   {:task-of  (fn [unit] unit)
    :session! (fn [host unit] ((:write-file host) unit (str "(ns " agent ")")))}))

(defn simulate
  "Run the scenario. Returns {:materialized [work…] :held [work…] :racer status
  :db db :now now}. `now` is the governor's logical time (default 1000, past the
  stale lease's TTL)."
  [& [{:keys [now] :or {now 1000}}]]
  (let [db           (store/mem-store)
        materialized (atom [])
        held         (atom [])
        act          (actor/build {:now now :materialize #(swap! materialized conj (:work %))})]
    ;; queue all work-units
    (doseq [[_ unit] work] (agent/enqueue! db {:unit unit :created-by "root"}))
    ;; each agent leases its unit, runs a coding session, proposes
    (doseq [[agent unit ttl] work]
      (agent/claim-and-propose! db {:unit unit :agent agent :ttl-ms ttl :now 0
                                    :run (coding-run-for agent)}))
    ;; a racer contends for src/a.clj — must lose and never run
    (let [racer (agent/claim-and-propose!
                 db {:unit "src/a.clj" :agent "pc2-racer" :ttl-ms 100000 :now 1
                     :run (fn [_] (throw (ex-info "racer should never run its session" {})))})]
      ;; drive the coordinator until every proposal is governed (auto-resume sign-offs)
      (loop [i 0]
        (when (and (seq (gov/pending-proposals db)) (< i 100))
          (let [r (g/run* act {:db db} {:thread-id (str "t" i) :now now})
                st (get-in r [:state :decision])]
            (when (= :hold (:kind st))
              (swap! held conj (get-in st [:proposal :work])))
            (when (= :interrupted (:status r))
              (g/run* act nil {:thread-id (str "t" i)})))  ; human signs off
          (recur (inc i))))
      {:materialized (vec (sort @materialized))
       :held         (vec (sort @held))
       :racer        (:status racer)
       :db db :now now})))

(defn -main [& _]
  (let [{:keys [materialized held racer db now]} (simulate {})
        snap (view/snapshot db now)]
    (println "── fleet coordination: 8 agents, 1 repo, one governed writer ──\n")
    (println "materialized to git (governor-accepted):")
    (doseq [w materialized] (println "  ✓" w))
    (println "\nheld (governor rejected — never reached git):")
    (doseq [w held] (println "  ✗" w))
    (println (str "\nracer for src/a.clj: " racer " (lock-free claim exclusion — never ran)"))
    (println (str "\nsnapshot: datoms=" (:datoms snap)
                  " pending=" (:pending snap)
                  " works=" (count (:works snap))))))
