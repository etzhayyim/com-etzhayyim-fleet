(ns fleet.deploy
  "Deploy entrypoint — wires a REAL Murakumo-fleet LLM (langchain.model
  OpenAI-compatible against the local Ollama, gemma-4-E4B) into
  `fleet.advisor/llm-picker` and drains a small pending-proposal queue
  through ONE FleetCoordinatorActor end to end (pick -> govern ->
  materialize/hold/sign-off).

  Same shape as `tashikame.deploy`/`kouhou.deploy`/`yosoku.deploy`: this only
  proves the real-LLM -> FleetGovernor path against the live Murakumo model.
  `:materialize` stays a MOCK (in-memory) here, same as `fleet.sim`'s own
  capstone demo — fleet's real materialize hook is a live git writer, and
  flipping that on is an operational decision well outside proving the
  picker wiring, not something this entrypoint does.

  Usage: clojure -M:dev -m fleet.deploy
  Env:   FLEET_OLLAMA_URL (default http://127.0.0.1:11434)
         FLEET_OLLAMA_MODEL (default gemma-4-E4B qat)"
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [langchain.model :as model]
            [langgraph.graph :as g]
            [fleet.actor :as actor]
            [fleet.advisor :as advisor]
            [kotoba.fleet.governor :as kgov]
            [kotoba.fleet.lease :as lease]
            [kotoba.fleet.store :as store])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers])
  (:gen-class))

(def ^:private default-ollama-url
  (or (System/getenv "FLEET_OLLAMA_URL") "http://127.0.0.1:11434"))

(def ^:private default-ollama-model
  (or (System/getenv "FLEET_OLLAMA_MODEL")
      "hf.co/unsloth/gemma-4-E4B-it-qat-GGUF:UD-Q4_K_XL"))

(defn jvm-http-fn
  "langchain.model :http-fn backed by the JDK HTTP client (no dependency)."
  [{:keys [url method headers body]}]
  (let [b (HttpRequest/newBuilder (URI/create url))]
    (doseq [[k v] headers] (.header b k v))
    (let [req  (-> b (.method (str/upper-case (name (or method :post)))
                             (if body
                               (HttpRequest$BodyPublishers/ofString body)
                               (HttpRequest$BodyPublishers/noBody)))
                   (.build))
          resp (.send (HttpClient/newHttpClient) req (HttpResponse$BodyHandlers/ofString))]
      {:status (.statusCode resp) :body (.body resp)})))

(defn ollama-chat-model
  "Build a langchain.model/openai-model against a Murakumo-fleet Ollama.
  Refuses non-Murakumo hosts (Rider v3.3 §2(i))."
  ([]
   (ollama-chat-model default-ollama-url default-ollama-model))
  ([ollama-url ollama-model]
   (advisor/assert-murakumo! ollama-url)
   (model/openai-model
    {:url        (str ollama-url "/v1/chat/completions")
     :model      ollama-model
     :api-key    nil
     :http-fn    jvm-http-fn
     :json-write json/write-str
     :json-read  #(json/read-str % :key-fn keyword)})))

(def ^:private demo-queue
  "[agent unit] — three candidates, one under a protected path, so the demo
  proves the real picker's choice still routes through sign-off correctly."
  [["pc1-a1" "src/a.clj"]
   ["pc2-a2" "src/b.clj"]
   ["pc1-a3" "manifest/west.yml"]])

(defn -main [& _]
  (let [chat  (ollama-chat-model)
        pick  (advisor/llm-picker chat {:max-tokens 256})
        db    (store/mem-store)
        wrote (atom [])
        act   (actor/build {:now 100 :materialize #(swap! wrote conj (:work %)) :advise pick})]
    (doseq [[agent unit] demo-queue]
      (lease/claim! db {:work unit :agent agent :ttl-ms 100000 :now 0})
      (kgov/submit-proposal! db {:work unit :agent agent :payload 1 :now 1}))
    (println "=== fleet deploy (real LLM picker @ Murakumo) ===")
    (loop [i 0]
      (when (and (seq (kgov/pending-proposals db)) (< i 10))
        (let [r (g/run* act {:db db} {:thread-id (str "t" i) :now 100})]
          (println "tick" i "status:" (:status r) "decision:" (get-in r [:state :decision :kind]))
          (when (= :interrupted (:status r))
            (g/run* act nil {:thread-id (str "t" i)})))
        (recur (inc i))))
    (println "materialized:" (pr-str @wrote))))
