(ns fleet.actor
  "FleetCoordinatorActor — one supervised coordinator = one langgraph-clj
  StateGraph. The coordinator intelligence node is sealed into `:coordinate`;
  its pick is ALWAYS routed through the FleetGovernor before anything is
  materialized to git. One graph run = one coordination tick:

    observe → coordinate → govern → decide → materialize | hold | (human-signoff)

  Writes to protected paths pause on `interrupt-before #{:human-signoff}` — real
  human-in-the-loop, exactly like robotaxi-actor's teleop handoff. The single
  git writer is the injected `:materialize` hook, reached only for a proposal the
  FleetGovernor accepted (lease-held, gate-passed). The `:audit` channel is the
  append-only ledger of every decision."
  (:require [langgraph.graph :as g]
            [langgraph.checkpoint :as cp]
            [fleet.coordinator :as coord]
            [fleet.governor :as gov]
            [kotoba.fleet.governor :as kgov]))

(defn build
  "Compile a FleetCoordinatorActor graph.
  opts: {:checkpointer cp :policy p :gate fn :now t :materialize fn :advise fn}."
  [& [{:keys [checkpointer policy gate now materialize advise]
       :or   {checkpointer (cp/mem-checkpointer)
              policy        gov/default-policy}}]]
  (-> (g/state-graph
       {:channels
        {:db       {:default nil}     ; the fleet :db-api (kotoba-fleet store)
         :proposal {:default nil}     ; coordinator's pick (proposal only)
         :verdict  {:default nil}     ; FleetGovernor censor result
         :decision {:default nil}     ; :materialize | :signoff | :hold
         :audit    {:reducer into :default []}}})

      (g/add-node :observe (fn [s] s))

      ;; contained intelligence — proposes, never materializes
      (g/add-node :coordinate
        (fn [{:keys [db]}]
          (let [p (coord/propose db {:advise advise})]
            {:proposal p
             :audit    [{:t :propose :proposal (:id p) :agent (:agent p)}]})))

      ;; independent censor
      (g/add-node :govern
        (fn [{:keys [db proposal]}]
          {:verdict (gov/check db proposal {:policy policy :gate gate :now now})}))

      (g/add-node :decide
        (fn [{:keys [verdict proposal]}]
          (if (:ok? verdict)
            {:decision {:kind (if (:needs-signoff verdict) :signoff :materialize)
                        :proposal proposal}}
            {:decision {:kind :hold :proposal proposal}
             :audit    [{:t :govern-hold :reason (:reason verdict)
                         :proposal (:id proposal)}]})))

      ;; escalation — paused by interrupt-before; a human resumes to release
      (g/add-node :human-signoff
        (fn [{:keys [proposal]}]
          {:audit [{:t :human-signoff :proposal (:id proposal)}]}))

      ;; the ONLY writer that touches git — reached only for accepted proposals.
      ;; Records an :accepted receipt so the proposal drains once (never re-picked).
      (g/add-node :materialize
        (fn [{:keys [db proposal]}]
          (when materialize (materialize proposal))
          (kgov/record! db {:proposal-id (:id proposal) :work (:work proposal)
                            :verdict :accepted})
          {:audit [{:t :materialize :proposal (:id proposal) :work (:work proposal)}]}))

      ;; held proposals are receipted :rejected — also drained once, with the reason
      (g/add-node :hold
        (fn [{:keys [db proposal verdict]}]
          (kgov/record! db {:proposal-id (:id proposal) :work (:work proposal)
                            :verdict :rejected :reason (:reason verdict)})
          {:audit [{:t :hold :proposal (:id proposal) :reason (:reason verdict)}]}))

      (g/add-node :done (fn [s] s))

      (g/set-entry-point :observe)
      (g/add-edge :observe :coordinate)
      (g/add-edge :coordinate :govern)
      (g/add-edge :govern :decide)
      (g/add-conditional-edges :decide
        (fn [{:keys [decision]}]
          (case (:kind decision)
            :materialize :materialize
            :signoff     :human-signoff
            :hold        :hold)))
      (g/add-edge :human-signoff :materialize)
      (g/add-edge :materialize :done)
      (g/add-edge :hold :done)
      (g/set-finish-point :done)

      (g/compile-graph
       {:checkpointer     checkpointer
        :interrupt-before #{:human-signoff}})))
