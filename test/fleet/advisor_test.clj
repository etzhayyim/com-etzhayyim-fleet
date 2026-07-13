(ns fleet.advisor-test
  "`mock-picker`/`llm-picker` unit tests use plain proposal maps (the picker
  only ever reads `:proposal/id`/`:proposal/work`/`:proposal/agent` — no
  store dependency). `llm-picker`'s tests use `langchain.model/mock-model`
  (fully offline/deterministic, no network) — they prove the wiring, prompt
  building, and fallback-to-mock-picker behavior, NOT anything about a live
  Murakumo endpoint. The final integration test wires `llm-picker` into a
  real `fleet.actor/build` graph against `kotoba.fleet.governor`-backed
  proposals to prove the governor still gates identically no matter which
  proposal the picker chose."
  (:require [clojure.test :refer [deftest is testing]]
            [langchain.model :as model]
            [langgraph.graph :as g]
            [fleet.actor :as actor]
            [fleet.advisor :as advisor]
            [kotoba.fleet.governor :as kgov]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.store :as store]))

(def p1 {:proposal/id "p1" :proposal/work "src/a.clj" :proposal/agent "A"})
(def p2 {:proposal/id "p2" :proposal/work "src/b.clj" :proposal/agent "B"})
(def p3 {:proposal/id "p3" :proposal/work "manifest/west.yml" :proposal/agent "C"})

(deftest mock-picker-picks-earliest
  (is (= p1 (advisor/mock-picker [p1 p2 p3])))
  (is (nil? (advisor/mock-picker []))))

(deftest trace-is-a-decision-grounded-audit-record
  (let [tr (advisor/trace [p1 p2] p1 "earliest")]
    (is (= :advisor-pick (:t tr)))
    (is (= ["p1" "p2"] (:candidates tr)))
    (is (= "p1" (:chosen tr)))
    (is (= "earliest" (:rationale tr)))))

;; ───────────────────────── assert-murakumo! ─────────────────────────

(deftest assert-murakumo-accepts-allowlisted-hosts
  (advisor/assert-murakumo! "http://127.0.0.1:11434")
  (advisor/assert-murakumo! "http://192.168.1.70:4000"))

(deftest assert-murakumo-accepts-tailnet-fleet-nodes
  (testing "com-junkawasaki tailnet Murakumo-fleet Ollama nodes (verified live
            2026-07-13) are the same physical fleet as the LAN entries, just
            reachable via Tailscale"
    (doseq [ip ["100.98.142.59" "100.66.28.79" "100.102.78.81" "100.75.169.8"
                "100.89.204.30" "100.82.123.35" "100.101.27.85" "100.81.66.86"]]
      (advisor/assert-murakumo! (str "http://" ip ":11434")))))

(deftest assert-murakumo-rejects-other-hosts
  (is (thrown? Exception (advisor/assert-murakumo! "https://api.openai.com"))))

;; ───────────────────────── parse-pick-edn ─────────────────────────

(def ^:private known #{"p1" "p2" "p3"})

(deftest parse-pick-accepts-well-formed-choice
  (is (= {:proposal-id "p2" :rationale "smaller unit"}
         (advisor/parse-pick-edn "{:proposal-id \"p2\" :rationale \"smaller unit\"}" known))))

(deftest parse-pick-strips-code-fences
  (is (= "p2" (:proposal-id (advisor/parse-pick-edn "```edn\n{:proposal-id \"p2\"}\n```" known)))))

(deftest parse-pick-rejects-out-of-vocabulary-id
  (is (nil? (advisor/parse-pick-edn "{:proposal-id \"p99\"}" known))))

(deftest parse-pick-rejects-unparseable-content
  (is (nil? (advisor/parse-pick-edn "not edn {{{" known))))

;; ───────────────────────── llm-picker (offline, mock-model) ─────────────────────────

(deftest llm-picker-returns-the-chosen-candidate
  (let [chat (model/mock-model
              [{:role :assistant
                :content "{:proposal-id \"p2\" :rationale \"non-protected, smaller unit\"}"}])
        pick (advisor/llm-picker chat)]
    (is (= p2 (pick [p1 p2 p3])))))

(deftest llm-picker-falls-back-to-mock-picker-on-unparseable-output
  (let [chat (model/mock-model [{:role :assistant :content "I refuse to answer."}])
        pick (advisor/llm-picker chat)]
    (is (= p1 (pick [p1 p2 p3])))))

(deftest llm-picker-falls-back-to-mock-picker-on-out-of-vocabulary-id
  (let [chat (model/mock-model [{:role :assistant :content "{:proposal-id \"nope\"}"}])
        pick (advisor/llm-picker chat)]
    (is (= p1 (pick [p1 p2 p3])))))

(deftest llm-picker-falls-back-to-mock-picker-on-chat-model-error
  (let [chat (model/mock-model (fn [_ _] (throw (ex-info "network down" {}))))
        pick (advisor/llm-picker chat)]
    (is (= p1 (pick [p1 p2 p3])))))

(deftest llm-picker-empty-pending-is-nil-not-an-error
  (let [chat (model/mock-model (fn [_ _] (throw (ex-info "should never be called" {}))))
        pick (advisor/llm-picker chat)]
    (is (nil? (pick [])))))

;; ───────────────────────── integration: llm-picker inside the real actor ─────────────────────────

(deftest llm-picker-inside-fleet-actor-governor-still-gates-the-chosen-proposal
  (testing "the picker only reorders triage — a protected-path proposal it
            chooses (real or fallen-back-to-mock-picker; the LLM here cannot
            know the store-generated id in advance, so it necessarily falls
            back) still pauses for human sign-off, exactly as the default
            earliest-first picker would, and still materializes on sign-off"
    (let [db (store/mem-store)
          wrote (atom [])
          chat (model/mock-model
                [{:role :assistant
                  :content "{:proposal-id \"unknowable-in-advance\" :rationale \"n/a\"}"}])
          pick (advisor/llm-picker chat)
          act (actor/build {:now 100 :materialize #(swap! wrote conj (:work %))
                            :advise pick})]
      (lease/claim! db {:work "manifest/west.yml" :agent "A" :ttl-ms 99999 :now 0})
      (kgov/submit-proposal! db {:work "manifest/west.yml" :agent "A" :payload 1 :now 1})
      (let [r (g/run* act {:db db} {:thread-id "t1" :now 100})]
        (is (= :interrupted (:status r))
            "protected path still pauses for human sign-off no matter which picker chose it")
        (g/run* act nil {:thread-id "t1"})
        (is (= ["manifest/west.yml"] @wrote) "human sign-off still materializes")
        (is (empty? (kgov/pending-proposals db)) "the proposal drains after materializing")))))
