(ns fleet.governor
  "FleetGovernor — the independent censor that earns the coordinator the right to
  materialize a write to git. Different system from the coordinator (that is the
  whole point of the actor pattern). It enforces the single fleet invariant:

    a proposal materializes ONLY IF the proposing agent currently holds the
    lease on its work-unit AND a policy gate passes — and writes to protected
    paths additionally require human sign-off.

  Everything else is HELD (append-only), never silently dropped or overwritten."
  (:require [clojure.string :as str]
            [kotoba.fleet.lease :as lease]))

(def default-policy
  "Writes under these path prefixes require human sign-off before materializing
  (manifest + ADR = fleet's own control surface; treat as protected)."
  {:protected-paths #{"manifest/" "90-docs/adr/"}})

(defn- protected?
  [policy work]
  (boolean (some #(str/starts-with? (str work) %) (:protected-paths policy))))

(defn check
  "Censor a coordinator `proposal` against the live lease state and policy.
  Returns {:ok? bool :needs-signoff bool :reason kw}. `opts`:
    :policy  — protected-path policy (default `default-policy`)
    :gate    — (fn [proposal] truthy?) domain accept/reject (default accept)
    :now     — logical time for lease liveness."
  [db proposal {:keys [policy gate now]}]
  (if (nil? proposal)
    {:ok? false :needs-signoff false :reason :no-proposal}
    (let [policy (or policy default-policy)
          gate   (or gate (constantly true))
          {:keys [work agent]} proposal
          holder (lease/holder db work now)]
      (cond
        (not= holder agent) {:ok? false :needs-signoff false
                             :reason :not-lease-holder :holder holder}
        (not (gate proposal)) {:ok? false :needs-signoff false :reason :gate-rejected}
        (protected? policy work) {:ok? true :needs-signoff true :reason :protected-path}
        :else {:ok? true :needs-signoff false :reason :accepted}))))
