(ns fleet.runner
  "kotoba-code integration seam — turn a bounded coding session into the fleet
  agent's injected `run` fn (kotoba.fleet.agent/claim-and-propose! `:run`).

  A coding agent's writes must become PROPOSAL DATA, never a git write: agents
  only propose, the FleetGovernor materializes. So we run the session against a
  CAPTURING host whose `write_file` records edits into an atom instead of the
  filesystem. The session driver is injected (`:session!`) so this ns compiles
  and tests WITHOUT a hard kotoba-code dependency; wire the real agent in prod:

    (require '[kotoba-code.agent :as kc])
    (coding-run
      {:base-files serve-work-unit-files
       :session! (fn [host task]
                   (kc/run-task (kc/build-agent {:model model :host host
                                                 :recursion-limit 40})
                                task))})   ; or kotoba-code.gate/run-gated for red/green

  Live wiring is currently blocked by two things outside this repo: kotoba-code's
  own deps.edn local-roots point at a non-existent path (orgs/kotoba-lang/
  langchain-clj — the lib is at com-junkawasaki/langchain-clj), and a live model
  needs OR_KEY (OpenRouter) or the local Murakumo gateway. The seam below is
  ready; only the injected `:session!` needs those resolved."
  (:require [clojure.string :as str]))

(defn capturing-host
  "A kotoba-code capability host whose `:write-file` CAPTURES edits into
  `captured` (a {path→content} atom) instead of touching disk — so a session's
  writes become proposal data with no git side effect. `:read-file` serves the
  captured version if present, else the work-unit's `base` file-set; the
  shell/test/network seams are inert."
  [base captured]
  {:read-file   (fn [p]   (or (get @captured p) (get base p) ""))
   :write-file  (fn [p c] (swap! captured assoc p c) (str "captured " p))
   :run-clojure (fn [_]   "nil")
   :run-tests   (fn []    "Ran 0 tests containing 0 assertions.\n0 failures, 0 errors.")
   :list-dir    (fn [_]   (str/join "\n" (keys base)))
   :search      (fn [_]   "")
   :rollback    (fn []    :noop)})

(defn coding-run
  "Build a fleet `run` fn from a coding session. Opts:
    :session!   (fn [host task]) — drives ONE bounded coding session against the
                capability host (inject kotoba-code build-agent+run-task here).
    :base-files (fn [unit] → {path→content}) — the work-unit's current file-set.
    :task-of    (fn [unit] → task-string) — how a unit becomes a coding task.
  Returns (fn [unit] → {path→new-content} | nil): the captured writes as a
  proposal payload for the FleetGovernor, or nil when the session wrote nothing."
  [{:keys [session! base-files task-of]
    :or   {base-files (constantly {})
           task-of    (fn [unit] (str "Complete the coding task for work-unit: " unit
                                      ". Make edits with write_file; finish with DONE."))}}]
  (fn [unit]
    (let [captured (atom {})
          host     (capturing-host (base-files unit) captured)]
      (session! host (task-of unit))
      (not-empty @captured))))
