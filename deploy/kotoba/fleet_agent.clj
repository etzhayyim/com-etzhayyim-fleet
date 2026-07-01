(ns fleet-agent
  "kotoba WASM lattice component — a fleet AGENT. The mesh fires `on-tick` on the
  interval declared in fleet.app.edn; each firing runs ONE agent round over the
  shared kotoba graph: claim open work, run the coding session, propose. Pure
  datom ops (kotoba.fleet.agent) — NO git, NO sh — so it compiles clj→WASM
  (kotoba-clj) and runs on the lattice via `kotoba app deploy`, not a LaunchAgent
  shelling clojure.

  The governor (the single git writer) is NOT a WASM guest: git materialization
  is a capability-gated host operation, so exactly one host-side governor process
  drains proposals (see docs/DEPLOY.md). Agents — the ~20 that just lease+propose
  over the graph — are this component, scaled across the mesh."
  (:require [kotoba.fleet.agent :as agent]))

(defn- pick-agent [agents i] (nth agents (mod i (count agents))))

(defn on-tick
  "One agent round. `ctx` (mesh-provided): :db shared store (kotoba graph via the
  host db-api), :agents ids for this node, :run coding session, :now. Claims each
  open work-unit for a node agent and proposes; returns ctx with :fleet/proposed."
  [{:keys [db agents run now] :as ctx}]
  (let [proposed (->> (agent/open-work db now)
                      (map :work/unit)
                      (map-indexed
                       (fn [i unit]
                         (agent/claim-and-propose!
                          db {:unit unit :agent (pick-agent agents i)
                              :ttl-ms 600000 :now now :run run})))
                      (filter (fn [r] (= :proposed (:status r))))
                      count)]
    (assoc ctx :fleet/proposed proposed)))

(defn run
  "Component entry (kotoba-component world) — one round's worth of work."
  [ctx]
  (on-tick ctx))
