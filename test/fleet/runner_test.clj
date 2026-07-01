(ns fleet.runner-test
  "The kotoba-code seam: a coding session's writes are CAPTURED as a proposal
  payload (never a git write), and that payload flows through the fleet agent
  into a governed proposal. The session is mocked here (the real kotoba-code
  build-agent+run-task plugs into the same `:session!` slot)."
  (:require [clojure.test :refer [deftest is testing]]
            [fleet.runner :as runner]
            [kotoba.fleet.agent :as agent]
            [kotoba.fleet.governor :as gov]
            [kotoba.fleet.store :as store]))

(deftest captures-writes-as-proposal-payload
  (testing "session writes via the host → returned as {path→content}, no git touched"
    (let [seen (atom nil)
          session! (fn [host task]
                     (reset! seen task)
                     ((:write-file host) "src/a.clj" "(ns a)")
                     ((:write-file host) "src/b.clj" "(ns b)"))
          run (runner/coding-run {:session! session!
                                  :base-files (constantly {"src/a.clj" "(ns a-old)"})})]
      (is (= {"src/a.clj" "(ns a)" "src/b.clj" "(ns b)"} (run "unit/x")))
      (is (re-find #"unit/x" @seen) "the work-unit is carried into the task"))))

(deftest no-writes-yields-nil
  (testing "a session that writes nothing proposes nothing"
    (let [run (runner/coding-run {:session! (fn [_ _] nil)})]
      (is (nil? (run "unit/x"))))))

(deftest read-serves-base-then-own-writes
  (testing "capturing host reads the base file-set, then its own captured edits"
    (let [captured (atom {})
          host (runner/capturing-host {"f" "base"} captured)]
      (is (= "base" ((:read-file host) "f")))
      ((:write-file host) "f" "new")
      (is (= "new" ((:read-file host) "f")) "later reads see the captured write"))))

(deftest coding-run-feeds-the-governed-agent
  (testing "captured session → fleet agent proposal → pending for the governor"
    (let [db (store/mem-store)
          session! (fn [host _] ((:write-file host) "src/a.clj" "(ns a)"))
          run (runner/coding-run {:session! session!})]
      (agent/enqueue! db {:unit "src/a.clj" :created-by "root"})
      (let [r (agent/claim-and-propose!
               db {:unit "src/a.clj" :agent "A" :ttl-ms 999 :now 0 :run run})]
        (is (= :proposed (:status r)))
        (is (= {"src/a.clj" "(ns a)"}
               (:proposal/payload (first (gov/pending-proposals db))))
            "the coding session's output is the governed proposal payload")))))
