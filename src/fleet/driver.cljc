(ns fleet.driver
  "Durable outer loop + multi-node roles (ADR-2606302000 F1/F2/F4).

  A StateGraph models ONE coordination tick; THIS is the repeating driver around
  it, split into the two roles a deployment needs:

    agent-round!  — runs on EVERY node: idle agents claim open work, run a bounded
                    coding session (injected `run` fn), and propose. Many nodes may
                    run this against ONE shared Datom graph; the optimistic lease
                    resolves cross-node contention deterministically — no extra
                    coordination, no lock server.
    govern!       — runs on exactly ONE node: the FleetCoordinatorActor drains all
                    pending proposals, materializing accepted writes (the single
                    git writer), closing their work-units, releasing leases. One
                    governor per repo preserves the single-writer invariant across
                    the whole fleet.

  `run-node!` composes both for the single-node case. Bounded by `:budget`.
  Crash-recoverable with NO bespoke code: a crashed agent's held lease expires
  (TTL) and its work reopens; all state is the append-only log (persisted on the
  kotoba-db backend, so recovery survives process/machine restarts)."
  (:require [langgraph.graph :as g]
            [fleet.actor :as actor]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.lease :as lease]))

(defn agent-round!
  "One agent round on a node: pair this node's `agents` (round-robin) with the
  currently-open work and claim → run → propose each. Returns the outcomes. Safe
  to run on many nodes against one shared store — losers of the optimistic claim
  simply back off. Opts: :agents :run :ttl-ms :now."
  [db {:keys [agents run ttl-ms now] :or {ttl-ms 600000}}]
  (assert (seq agents) "agent-round! needs at least one agent")
  (mapv (fn [[ag unit]]
          (agent/claim-and-propose!
           db {:unit unit :agent ag :ttl-ms ttl-ms :now now :run run}))
        (map vector (cycle agents) (map :work/unit (agent/open-work db now)))))

(defn govern!
  "Run the governor (the single git writer for a repo) until no proposals remain
  pending, auto-resuming sign-offs. `:materialize` is the real git write (default
  no-op); it is wrapped to also close the work-unit and release its lease. Returns
  the vector of materialized work-units. Opts: :now :policy :gate :materialize."
  [db {:keys [now policy gate materialize]}]
  (let [done (atom [])
        act  (actor/build
              {:now now :policy policy :gate gate
               :materialize (fn [{:keys [work agent] :as p}]
                              (when materialize (materialize p))
                              (swap! done conj work)
                              (agent/close-work! db work)
                              (lease/release! db {:work work :agent agent :now now}))})]
    (loop [i 0]
      (when (and (seq (gov/pending-proposals db)) (< i 1000))
        (let [tid (str "gov-" now "-" i)
              r   (g/run* act {:db db} {:thread-id tid :now now})]
          (when (= :interrupted (:status r))
            (g/run* act nil {:thread-id tid})))
        (recur (inc i))))
    @done))

(defn run-node!
  "Single-node durable loop = agent-round! + govern! per round until the queue is
  drained or `:budget` rounds elapse. Opts: :agents :run :budget :ttl-ms :now-fn
  :policy :gate. Returns {:materialized [work…] :rounds n :remaining open-count}."
  [db {:keys [agents run budget ttl-ms now-fn policy gate materialize]
       :or   {budget 100 ttl-ms 600000 now-fn (fn [r] (* (inc r) 1000))}}]
  (assert (seq agents) "run-node! needs at least one agent")
  (let [all (atom [])]
    (loop [round 0]
      (let [now  (now-fn round)
            open (agent/open-work db now)]
        (if (or (>= round budget)
                (and (empty? open) (empty? (gov/pending-proposals db))))
          {:materialized @all :rounds round :remaining (count (agent/open-work db now))}
          (do
            (agent-round! db {:agents agents :run run :ttl-ms ttl-ms :now now})
            (swap! all into (govern! db {:now now :policy policy :gate gate
                                         :materialize materialize}))
            (recur (inc round))))))))
