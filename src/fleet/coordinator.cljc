(ns fleet.coordinator
  "The *contained intelligence node*. It proposes which pending agent-write to
  advance next — a PROPOSAL only, never a materialization. Here it is a
  deterministic mock (earliest pending proposal in causal order); swap in an LLM
  advisor (langchain.model) via the `:advise` injection without touching the
  actor graph or the FleetGovernor. Like AR1 in robotaxi-actor, its output is
  always censored downstream."
  (:require [kotoba.fleet.governor :as kgov]))

(defn propose
  "Pick the next pending proposal to advance. `opts` may carry `:advise`, a fn
  of (pending-proposals) → chosen entity, to override the default earliest-first
  policy with a real advisor. Returns a normalized proposal map or nil."
  [db {:keys [advise]}]
  (let [pending (kgov/pending-proposals db)
        chosen  (if advise (advise pending) (first pending))]
    (when chosen
      {:id      (:proposal/id chosen)
       :work    (:proposal/work chosen)
       :agent   (:proposal/agent chosen)
       :payload (:proposal/payload chosen)})))
