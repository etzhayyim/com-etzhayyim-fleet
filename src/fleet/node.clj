(ns fleet.node
  "Env-driven launcher for ONE fleet node (ADR-2606302000 F4 deployment glue).

  A node reads its config from the environment, then executes its role for one
  bounded round — cron / systemd-timer friendly: each round is bounded and all
  state lives in the shared append-only log, so rounds are resumable and crash
  safe (invoke every N seconds; a crashed round just leaves an expiring lease).

    FLEET_ROLE    agent | governor | both     (default both — local dev)
    FLEET_AGENTS  comma-separated agent ids    (default a1,a2)
    FLEET_BUDGET  max rounds when role=both     (default 20)
    FLEET_GRAPH   shared IPNS name             (→ kotoba-db backend, see below)
    OR_KEY        OpenRouter key               (→ live kotoba-code session)

  The default path (no FLEET_GRAPH / OR_KEY) is a self-contained local smoke test:
  in-memory store + an echo `run` fn, seeded with demo work, so `clojure -M:dev -m
  fleet.node` drains a queue and prints — no creds, no network.

  PRODUCTION wiring (the creds step, see docs/DEPLOY.md) constructs the two
  injections and calls `run-role!` directly:

    (require '[kotoba.fleet.kotoba-store :as ks] '[langchain.kotoba-db :as kdb]
             '[fleet.worktree :as wt])
    (def db  (ks/db-api-store {:api (kdb/kotoba-api host-caps)
                               :conn (kdb/kotoba-conn url FLEET_GRAPH {:cacao … :did …})}))
    (def run (wt/worktree-run repo {:session! kotoba-code-session :test-cmd [\"clojure\" \"-M:test\"]}))
    (fleet.node/run-role! db {:role :agent :agents [\"pc1-a1\" …] :run run :now (now-ms)})"
  (:require [clojure.string :as str]
            [fleet.driver :as driver]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.store :as store]
            [kotoba.fleet.view :as view]))

(defn run-role!
  "Execute a node's role once against `db`. Opts:
    :role   :agent | :governor | :both
    :agents agent ids (for :agent / :both)
    :run    the fleet run fn (a coding session; echo stub in local dev)
    :budget max rounds for :both (default 20)
    :now    logical time; :policy :gate passed to the governor
  Returns a role-specific report."
  [db {:keys [role agents run budget now policy gate materialize]
       :or   {budget 20 now 0}}]
  (case (keyword role)
    :agent    {:role :agent
               :proposed (->> (driver/agent-round! db {:agents agents :run run :now now})
                              (filter #(= :proposed (:status %)))
                              count)
               :open (count (agent/open-work db now))}
    :governor {:role :governor
               :materialized (driver/govern! db {:now now :policy policy :gate gate
                                                 :materialize materialize})
               :pending (count (gov/pending-proposals db))}
    :both     (driver/run-node! db {:agents agents :run run :budget budget
                                    :policy policy :gate gate :materialize materialize})))

(defn- echo-run
  "Local-dev run fn: no coding model, just a deterministic edit so the loop drains."
  [node]
  (fn [unit] {unit (str ";; touched by fleet node " node " — unit " unit)}))

(defn -main
  "Local smoke by default (in-memory store + echo run, seeded demo queue).
  Set FLEET_GRAPH / OR_KEY + call `run-role!` directly for production (see ns doc)."
  [& _]
  (let [role   (or (System/getenv "FLEET_ROLE") "both")
        node   (or (System/getenv "FLEET_NODE") "local-dev")
        agents (str/split (or (System/getenv "FLEET_AGENTS") "a1,a2") #",")
        budget (Long/parseLong (or (System/getenv "FLEET_BUDGET") "20"))
        db     (store/mem-store)]
    (when (System/getenv "FLEET_GRAPH")
      (println "note: FLEET_GRAPH set, but this launcher runs the local in-memory smoke path;")
      (println "      wire the kotoba-db backend + kotoba-code session and call fleet.node/run-role! (see ns doc)."))
    (println (str "fleet node '" node "' role=" role " agents=" (str/join "," agents) " (local smoke)\n"))
    ;; seed a demo work queue
    (doseq [u ["src/a.clj" "src/b.clj" "src/c.clj" "manifest/west.yml" "src/d.clj"]]
      (agent/enqueue! db {:unit u :created-by "root"}))
    (let [report (run-role! db {:role role :agents agents :run (echo-run node)
                           :budget budget :now 1000})
          snap   (view/snapshot db 1000)]
      (println "role report:" (pr-str (dissoc report :materialized)))
      (when-let [m (:materialized report)]
        (println "materialized to git (governed):")
        (doseq [w (sort m)] (println "  ✓" w)))
      (println (str "\nsnapshot: datoms=" (:datoms snap)
                    " pending=" (:pending snap) " works=" (count (:works snap)))))))
