(ns fleet.driver
  "Durable outer loop for one fleet node — the bounded, crash-recoverable harness
  the ADR calls for. A StateGraph models ONE coordination tick; THIS is the
  repeating driver: each round every idle agent claims an open work-unit, runs a
  bounded coding session (the injected `run` fn — kotoba-code in prod), and
  proposes; the FleetCoordinatorActor then drains, materializing accepted writes,
  CLOSING their work-units, and releasing the leases.

  Bounded by a `:budget` (max rounds). Crash-recoverable because ALL state is the
  append-only log: restart the loop on the same store and a crashed agent's held
  lease simply expires (TTL), freeing its work for another agent next round — no
  bespoke recovery code. With the kotoba-db backend the store is persisted, so
  recovery survives process restarts too."
  (:require [langgraph.graph :as g]
            [fleet.actor :as actor]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.lease :as lease]))

(defn- drive-governor!
  "Drive the coordinator until no proposals remain pending (auto-resume sign-offs).
  The materialize hook is the single git writer: it records the write, closes the
  work-unit, and releases its lease."
  [db {:keys [now policy gate sink]}]
  (let [act (actor/build
             {:now now :policy policy :gate gate
              :materialize (fn [{:keys [work agent]}]
                             (swap! sink conj work)
                             (agent/close-work! db work)
                             (lease/release! db {:work work :agent agent :now now}))})]
    (loop [i 0]
      (when (and (seq (gov/pending-proposals db)) (< i 1000))
        (let [tid (str "gov-" now "-" i)
              r   (g/run* act {:db db} {:thread-id tid :now now})]
          (when (= :interrupted (:status r))
            (g/run* act nil {:thread-id tid})))
        (recur (inc i))))))

(defn run-node!
  "Run a node's durable loop over `db`. Opts:
    :agents  — agent-ids available on this node (round-robin onto open work)
    :run     — the fleet run fn (kotoba-code session; a mock in tests)
    :budget  — max rounds (default 100)
    :ttl-ms  — lease TTL (default 10 min); a crashed agent's work reopens after this
    :now-fn  — (fn [round] → logical-time) so leases age across rounds
    :policy :gate — passed to the FleetGovernor
  Returns {:materialized [work…] :rounds n :remaining open-work-count}."
  [db {:keys [agents run budget ttl-ms now-fn policy gate]
       :or   {budget 100 ttl-ms 600000 now-fn (fn [r] (* (inc r) 1000))}}]
  (assert (seq agents) "run-node! needs at least one agent")
  (let [sink (atom [])]
    (loop [round 0]
      (let [now  (now-fn round)
            open (agent/open-work db now)]
        (if (or (>= round budget)
                (and (empty? open) (empty? (gov/pending-proposals db))))
          {:materialized @sink
           :rounds round
           :remaining (count (agent/open-work db now))}
          (do
            ;; agent phase: pair idle agents with open work, claim → run → propose
            (doseq [[ag unit] (map vector (cycle agents) (map :work/unit open))]
              (agent/claim-and-propose!
               db {:unit unit :agent ag :ttl-ms ttl-ms :now now :run run}))
            ;; governor phase: drain (materialize closes + releases)
            (drive-governor! db {:now now :policy policy :gate gate :sink sink})
            (recur (inc round))))))))
