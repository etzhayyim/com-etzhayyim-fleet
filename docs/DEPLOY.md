# Deploying the fleet — 2 PCs × ~20 agents (ADR-2606302000 F4)

The coordination model needs **no bespoke distributed-systems code**: every node
points at ONE shared Datom graph, the optimistic lease resolves cross-node
contention, and exactly one node runs the governor. This is the runbook.

## Topology

```
                 ┌──────────── kotobase.net: ONE shared Datom graph ───────────┐
                 │  append-only [e a v t] log — the fleet's coordination state   │
                 │  :work · :lease · :proposal · :receipt   (IPNS/CID-addressed) │
                 └──▲───────────────▲───────────────────────────────▲───────────┘
      agent-round!  │               │ agent-round!             govern! │ (ONE node only)
        ┌───────────┴──┐       ┌────┴─────────┐            ┌───────────┴────────┐
        │ PC1           │       │ PC2          │            │ governor (PC1 or PC2)│
        │ ~10 agents    │       │ ~10 agents   │            │ FleetCoordinatorActor│
        │ durable driver│       │ durable driver│           │ → git single writer  │
        └───────────────┘       └──────────────┘            └──────────────────────┘
          murakumo `bb fleet <log.edn>` — coordination-plane view over the graph
```

- **Every node** runs `fleet.driver/agent-round!` in a loop: its ~10 agents claim
  open work, run a bounded kotoba-code session in an isolated **worktree**
  (`fleet.worktree/worktree-run`), and append proposals. Losers of the optimistic
  claim back off — no lock server.
- **Exactly one node** runs `fleet.driver/govern!`: the FleetCoordinatorActor
  drains proposals, materializes accepted writes to git (the single writer),
  closes the work-units, releases leases. Protected paths pause for human sign-off.
- **The shared graph is the only coordination substrate.** kotobase.net stores it
  as an append-only, IPNS/CID-addressed Datom log — that IS the "log replication":
  each node reads/writes the same key-derived graph, so state is consistent by
  content-addressing, not by a bespoke sync protocol.

## Shared graph (kotoba-db backend)

Point both nodes at the same graph via the store adapter
(`kotoba.fleet.kotoba-store/db-api-store`):

```clojure
(require '[langchain.kotoba-db :as kdb]
         '[kotoba.fleet.kotoba-store :as ks])
(def db (ks/db-api-store
          {:api  (kdb/kotoba-api host-caps)                       ; :http-fn :json-write :json-read
           :conn (kdb/kotoba-conn "https://kotobase.net" GRAPH    ; the fleet's shared IPNS name
                                  {:cacao CACAO_B64 :did DID})})) ; self-minted, :cap/transact
```

`GRAPH` is the fleet's key-derived IPNS name; the governor node's actor key
self-mints the CACAO (see `ai-gftd-itonami/src/itonami/cacao.clj`). All nodes use
the same `GRAPH`; agent nodes need `:cap/transact` for leases+proposals, the
governor node the same plus write access to the repo it materializes into.

## Provisioning with murakumo

`murakumo` already provisions the Mac fleet over Tailscale and installs resident
nodes (`bb provision`, `bb up`). For the fleet coordinator, per node:

```bash
# on the operator laptop — provision both PCs (installs kotoba + resident node)
bb provision all
bb up all

# each node runs its agent driver against the shared graph (env-configured):
FLEET_GRAPH=k51q…            # shared IPNS name
FLEET_ROLE=agent             # or `governor` on exactly one node
OR_KEY=…                     # OpenRouter (or murakumo: local gateway) for kotoba-code
clojure -M:dev -m fleet.node   # loops agent-round! (agent) / govern! (governor)

# watch the whole fleet from the operator laptop:
bb fleet <(kotoba graph export $FLEET_GRAPH)   # kotoba.fleet.view snapshot
```

`fleet.node/-main` is the env-driven launcher: it reads `FLEET_ROLE` /
`FLEET_AGENTS` / `FLEET_GRAPH` / `OR_KEY`, builds the store + run fn, and runs the
matching role for one bounded round (`run-role!`). The default no-creds path is a
local smoke; production sets `FLEET_GRAPH` + `OR_KEY` and wires the kotoba-db
store + kotoba-code session (see the ns doc).

### Residency — kotoba-native (agents) + host governor

Run the fleet **through the kotoba mesh, not `sh`**. The two roles residence
differently, because the governor writes git (a host operation) and the agents
don't:

**Agents (~20) — a kotoba WASM component.** They only lease + propose over the
graph (pure datom ops), so they compile clj→WASM and run as a scaled `on-tick`
component. The mesh IS the scheduler — no LaunchAgent, no `clojure` shell:

```bash
# deploy the agent component to the lattice (clj→WASM at --publish); the mesh
# fires fleet-agent/on-tick every interval on each eligible node.
kotoba app deploy deploy/kotoba/fleet.app.edn --publish --url http://<node>:8080
#   or fleet-wide, with drift-reconcile + self-heal:
murakumo bb reconcile murakumo.app.edn --apply          # add a :manifest entry → deploy/kotoba/fleet.app.edn
```

`deploy/kotoba/fleet-agent.clj` is the component (`on-tick` = one agent round);
`deploy/kotoba/fleet.app.edn` declares `:scale`, the `:tick` trigger, and
`:requires #{:cap/datom}`.

**Governor (exactly one) — a host process.** The single git writer can't be a
WASM guest; run it host-side on one node via `fleet.node` role=governor. This is
the only place the LaunchAgent/systemd unit is used:

```bash
deploy/install-node.sh --role governor --node pc1-gov --graph "$FLEET_GRAPH" --interval 30
deploy/install-node.sh --dry-run …          # render + print the unit, install nothing
```

`murakumo bb provision`/`bb up` place the kotoba binaries + the agent components +
the one governor unit across the Tailscale fleet; `deploy/systemd/fleet-node@.{service,timer}`
are the Linux equivalents for the governor. The rendered LaunchAgent is `plutil`-valid.

## Why this is safe at scale

| concern | mechanism |
|---|---|
| two nodes grab the same work | optimistic lease — earliest claim wins deterministically; loser backs off (`cross-node-lease-exclusion` test) |
| double git write across nodes | exactly one `govern!` node = single writer (`two-agent-nodes-one-governor` test) |
| a node/agent crashes mid-work | its lease TTL expires; the work reopens; another node picks it up (`crash-recovery-via-lease-ttl` test) |
| a bad edit reaches the repo | worktree gate — red tests → propose nothing; governor gate + protected-path sign-off |
| "who has the truth" | the content-addressed shared graph; no primary, no sync protocol |

## Scaling past 2 PCs

Add nodes: each just runs `agent-round!` against the same `GRAPH`. Keep exactly
one `govern!` node per repo (shard the governor by repo if you materialize into
many repos). No coordination code changes — the lease + single-graph model scales
by adding agent nodes.
