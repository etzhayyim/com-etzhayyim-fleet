(ns fleet.worktree-test
  "F3 isolation: a worktree gives an agent its own checkout to read/test against;
  the main tree is never touched, and a gate run is discarded with the worktree."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [fleet.worktree :as wt]))

(defn- temp-repo []
  (let [dir (str (System/getProperty "java.io.tmpdir") "fleet-wt-" (System/nanoTime))]
    (sh/sh "git" "init" "-q" "-b" "main" dir)
    (spit (str dir "/a.txt") "base-A")
    (spit (str dir "/src.clj") "(ns src)")
    (sh/sh "git" "-C" dir "add" "-A")
    (sh/sh "git" "-C" dir "-c" "user.email=t@t" "-c" "user.name=t" "commit" "-q" "-m" "init")
    dir))

(defn- rmrf [dir] (sh/sh "bash" "-c" (str "rm -rf " dir)))

(deftest worktree-isolates-reads-and-writes
  (let [repo (temp-repo)]
    (try
      (testing "read-files serves the worktree's committed file-set"
        (wt/with-worktree repo {:name "agent-1"}
          (fn [dir]
            (is (= {"a.txt" "base-A" "missing" nil}
                   (wt/read-files dir ["a.txt" "missing"])))
            (testing "edits inside the worktree do NOT touch the main tree"
              (wt/apply-writes! dir {"a.txt" "EDITED"})
              (is (= "EDITED" (slurp (str dir "/a.txt"))) "worktree sees the edit")
              (is (= "base-A" (slurp (str repo "/a.txt"))) "main tree is untouched")))))
      (testing "the worktree is gone after use (main tree still clean)"
        (is (not (.exists (io/file repo ".fleet-wt/agent-1"))))
        (is (= "base-A" (slurp (str repo "/a.txt")))))
      (finally (rmrf repo)))))

(deftest gate-runs-in-isolation-green-and-red
  (let [repo (temp-repo)]
    (try
      (wt/with-worktree repo {:name "agent-2"}
        (fn [dir]
          (testing "gate materializes writes + runs the test cmd, green when it passes"
            (let [g (wt/gate! dir {"src.clj" "(ns src) ;; fixed"}
                              ["bash" "-c" "echo 'Ran 1 tests containing 1 assertions.
0 failures, 0 errors.'"])]
              (is (:green? g))
              (is (= "(ns src) ;; fixed" (slurp (str dir "/src.clj"))) "write applied in worktree")))
          (testing "red gate is reported, not thrown"
            (let [g (wt/gate! dir {} ["bash" "-c" "echo 'FAIL 1 failures, 0 errors.'"])]
              (is (not (:green? g)))))))
      (is (= "(ns src)" (slurp (str repo "/src.clj"))) "main tree never saw the gate's edits")
      (finally (rmrf repo)))))

(deftest worktree-run-isolates-a-session-into-a-proposal
  (testing "worktree-run: session reads worktree base, writes captured as proposal, gated green"
    (let [repo (temp-repo)]
      (try
        (let [run (wt/worktree-run
                   repo
                   {:files-of (fn [_] ["src.clj"])
                    :session! (fn [host _unit]
                                (let [cur ((:read-file host) "src.clj")]      ; reads worktree base
                                  ((:write-file host) "src.clj" (str cur " ;; +edit"))))
                    :test-cmd ["bash" "-c" "echo '0 failures, 0 errors.'"]})]
          (is (= {"src.clj" "(ns src) ;; +edit"} (run "src.clj"))
              "captured write returned as proposal payload"))
        (is (= "(ns src)" (slurp (str repo "/src.clj"))) "main tree untouched by the session")
        (finally (rmrf repo))))))

(deftest worktree-run-red-gate-proposes-nothing
  (testing "a red gate suppresses the proposal (never propose a broken edit)"
    (let [repo (temp-repo)]
      (try
        (let [run (wt/worktree-run
                   repo
                   {:files-of (fn [_] ["src.clj"])
                    :session! (fn [host _] ((:write-file host) "src.clj" "broken"))
                    :test-cmd ["bash" "-c" "echo '2 failures, 0 errors.'"]})]
          (is (nil? (run "src.clj")) "red gate → nil payload → no proposal"))
        (finally (rmrf repo))))))

(deftest two-agents-two-worktrees-no-collision
  (let [repo (temp-repo)]
    (try
      (wt/with-worktree repo {:name "a"}
        (fn [da]
          (wt/with-worktree repo {:name "b"}
            (fn [dbb]
              (wt/apply-writes! da {"a.txt" "from-A"})
              (wt/apply-writes! dbb {"a.txt" "from-B"})
              (is (= "from-A" (slurp (str da "/a.txt"))))
              (is (= "from-B" (slurp (str dbb "/a.txt"))) "disjoint checkouts — no collision")))))
      (finally (rmrf repo)))))
