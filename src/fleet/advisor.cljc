(ns fleet.advisor
  "The *contained intelligence node* for `fleet.coordinator/propose`'s
  `:advise` injection — `(fn [pending-proposals] -> chosen-or-nil)`. Unlike
  yosoku/tashikame/kouhou's `Advisor` protocol, `coord/propose` only ever
  wants a bare picker fn (see `fleet.coordinator`'s own docstring: \"swap in
  an LLM advisor via the `:advise` injection\"), so this namespace matches
  that existing shape rather than introducing a protocol this repo never
  asked for.

  Picking is the ONLY thing an advisor here ever does — it selects which
  ALREADY-PENDING proposal to advance next; it never drafts a proposal's
  `:work`/`:agent`/`:payload` (those come from the agent's own lease + coding
  session, upstream of the coordinator entirely) and it never bypasses
  `fleet.governor`/`kotoba.fleet.governor` — a protected-path proposal still
  pauses for human sign-off, a lease-expired proposal is still held, no
  matter which proposal the picker chose first. `mock-picker` reproduces
  `coord/propose`'s own default (earliest pending, i.e. `first`) so tests can
  swap pickers without changing expected drain order when nothing else
  differs. `llm-picker` (bottom of this file) wires a real
  `langchain.model/ChatModel` against the Murakumo fleet, same
  `assert-murakumo!` host allowlist as `tashikame.advisor`/`kouhou.advisor`/
  `yosoku.advisor`."
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.string :as str]
            [langchain.model :as model]))

(defn mock-picker
  "The deterministic default picker: earliest pending proposal (by causal
  order — `pending` already arrives sorted by `:proposal/t`). Equivalent to
  `coord/propose`'s own `(if advise (advise pending) (first pending))`
  fallback, exposed here as a named fn so a test can inject it explicitly
  and still assert on default-order behavior."
  [pending]
  (first pending))

(defn trace
  "Decision-grounded audit record for a picker's choice (or non-choice)."
  [pending chosen rationale]
  {:t :advisor-pick
   :candidates (mapv :proposal/id pending)
   :chosen (:proposal/id chosen)
   :rationale rationale})

;; ───────────────────────── real-LLM picker (Murakumo fleet) ─────────────────────────
;; Sealed like every other advisor in this codebase: the LLM may only choose
;; among the proposal-ids it was actually shown — never invent one, never see
;; or influence a proposal's :work/:agent/:payload content. A hallucinated or
;; unparseable answer falls back to `mock-picker`'s earliest-first default,
;; never to nil (a picker returning nil short-circuits `coord/propose` to
;; "nothing to advance" — the safe fallback is choosing SOMETHING sane, not
;; choosing nothing).

(def allowed-infer-hosts
  "Murakumo-fleet inference hosts only (Rider v3.3 §2(i)) — the same
  allowlist as `tashikame.advisor`/`kouhou.advisor`/`yosoku.advisor`."
  #{"127.0.0.1:11434" "localhost:11434"
    "127.0.0.1:4000"  "localhost:4000"
    "192.168.1.70:4000"})

(defn- host-port [url]
  (when (string? url) (second (re-find #"(?i)^[a-z]+://([^/]+)" url))))

(defn assert-murakumo!
  "Throw if `ollama-url` is not a Murakumo-fleet inference host."
  [ollama-url]
  (let [hp (host-port ollama-url)]
    (when-not (contains? allowed-infer-hosts hp)
      (throw (ex-info (str "inference host " hp " is not Murakumo-fleet (Rider v3.3 §2(i))")
                      {:host hp})))))

(def fleet-picker-system-prompt
  "You triage a queue of pending git-write proposals from a multi-agent
fleet, one repo, one governed writer. Given a numbered list of candidates
(id, work path, agent), pick the ONE that should advance next -- prefer
non-protected-looking paths (e.g. not under manifest/) and smaller/safer-
looking units when in doubt; when truly unsure, prefer the earliest
(first-listed) candidate. Respond with ONLY a single-line EDN map, no
prose, no code fences:
  {:proposal-id \"<one of the given ids>\" :rationale \"one short sentence\"}")

(defn- candidate-line [i p]
  (str (inc i) ". id=" (:proposal/id p) " work=" (:proposal/work p)
       " agent=" (:proposal/agent p)))

(defn- build-prompt [pending]
  (str "Pending candidates:\n"
       (str/join "\n" (map-indexed candidate-line pending))
       "\n\nReturn ONLY the EDN map now."))

(defn parse-pick-edn
  "Defensively parse the LLM's `{:proposal-id :rationale}` EDN map. Returns
  nil (never a bogus pick) unless `:proposal-id` names one of `known-ids` —
  an out-of-vocabulary or unparseable answer is indistinguishable from `nil`
  to the caller, which falls back to `mock-picker`."
  [content known-ids]
  (let [cleaned (-> (str content)
                     (str/replace #"(?s)```[a-zA-Z]*" "")
                     (str/replace "```" ""))
        m (try (some-> (re-find #"(?s)\{.*\}" cleaned) edn/read-string)
               (catch #?(:clj Throwable :cljs :default) _ nil))
        pid (:proposal-id m)]
    (when (and (string? pid) (contains? known-ids pid))
      {:proposal-id pid :rationale (str (or (:rationale m) ""))})))

(defn llm-picker
  "Returns a `(fn [pending] -> chosen-or-nil)` backed by `chat-model`. Falls
  back to `mock-picker`'s earliest-first default on an empty `pending`, a
  hallucinated/unparseable LLM answer, or a chat-model error (fail-safe:
  triage order degrading to FIFO is always safe: `fleet.governor` gates the
  chosen proposal identically no matter how it was picked). gen-opts ->
  `model/-generate` opts (e.g. `{:max-tokens 256}`)."
  ([chat-model] (llm-picker chat-model {}))
  ([chat-model gen-opts]
   (fn [pending]
     (if (empty? pending)
       nil
       (let [known-ids (into #{} (map :proposal/id pending))
             picked
             (try
               (let [content (:content
                              (model/-generate chat-model
                                [{:role :system :content fleet-picker-system-prompt}
                                 {:role :user :content (build-prompt pending)}]
                                gen-opts))]
                 (parse-pick-edn content known-ids))
               (catch #?(:clj Exception :cljs :default) _ nil))]
         (if picked
           (some #(when (= (:proposal-id picked) (:proposal/id %)) %) pending)
           (mock-picker pending)))))))
