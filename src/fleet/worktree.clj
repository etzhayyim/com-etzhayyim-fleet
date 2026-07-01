(ns fleet.worktree
  "Per-agent git worktree isolation (ADR-2606302000 F3). Each agent reads and
  test-runs against its OWN detached checkout, so even reads and test runs never
  collide across the ~20 parallel agents — and the repo's main working tree is
  never touched. Writes still flow out as captured proposals (fleet.runner); the
  worktree is the isolated place a coding session READS current files and a gate
  VERIFIES the captured edits before they're proposed.

  JVM-only (shells `git worktree`), so this is `.clj`, not part of the portable
  `.cljc` core. Worktrees live under `<repo>/.fleet-wt/<name>` (gitignored) and
  are removed after use."
  (:require [clojure.java.shell :as sh]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [fleet.runner :as runner]))

(defn- git [repo & args]
  (let [r (apply sh/sh "git" "-C" repo args)]
    (when-not (zero? (:exit r))
      (throw (ex-info (str "git " (first args) " failed") (assoc r :repo repo))))
    r))

(defn add-worktree!
  "Create a detached worktree of `repo` at `dir`, checked out at `ref` (default HEAD)."
  [repo dir ref]
  (git repo "worktree" "add" "--detach" dir (or ref "HEAD"))
  dir)

(defn remove-worktree!
  "Remove the worktree at `dir` (force — discards its uncommitted edits)."
  [repo dir]
  (sh/sh "git" "-C" repo "worktree" "remove" "--force" dir)
  nil)

(defn with-worktree
  "Run `(f worktree-dir)` against a fresh isolated worktree of `repo` at `ref`
  (default HEAD), created under `<repo>/.fleet-wt/<name>`; always remove it after.
  `name` should be unique per agent/round (e.g. \"pc1-a3-42\")."
  [repo {:keys [ref name]} f]
  (let [dir (str repo "/.fleet-wt/" name)]
    (add-worktree! repo dir ref)
    (try (f dir) (finally (remove-worktree! repo dir)))))

(defn read-files
  "Read `paths` (repo-relative) from worktree `dir` → {path→content}; a missing
  file maps to nil. Serves as the capturing host's `base` (fleet.runner)."
  [dir paths]
  (into {} (for [p paths :let [f (io/file dir p)]]
             [p (when (.exists f) (slurp f))])))

(defn apply-writes!
  "Materialize captured `writes` ({path→content}) into worktree `dir` — for a
  test run only; the main tree and git history are untouched."
  [dir writes]
  (doseq [[p c] writes]
    (let [f (io/file dir p)]
      (io/make-parents f)
      (spit f c))))

(defn gate!
  "Apply captured `writes` into worktree `dir` and run `test-cmd` (a vector, e.g.
  [\"clojure\" \"-M:test\"]) there. Returns {:green? bool :out str}. Use inside
  `with-worktree` so the isolated checkout is discarded afterward — a red gate
  never leaves a broken tree, exactly like kotoba-code's own gate."
  [dir writes test-cmd]
  (apply-writes! dir writes)
  (let [r   (apply sh/sh (concat test-cmd [:dir dir]))
        out (str (:out r) (:err r))]
    {:green? (boolean (re-find #"0 failures, 0 errors" out))
     :out    out}))

(defn- sanitize [s] (str/replace (str s) #"[^A-Za-z0-9._-]" "_"))

(defn worktree-run
  "A fleet `run` fn (for kotoba.fleet.agent) that isolates each coding session in
  its OWN worktree — the F3 integration with fleet.runner. Per work-unit: open a
  fresh worktree, read the unit's file-set as the capturing host's base, run the
  injected `:session!`, optionally GATE the captured writes by running `:test-cmd`
  in that worktree, and return the writes as the proposal payload (nil on a red
  gate — never propose a broken edit). The worktree is always removed after.
  Opts: :session! :files-of (unit→paths, default [unit]) :test-cmd :ref."
  [repo {:keys [session! files-of test-cmd ref]
         :or   {files-of (fn [unit] [unit])}}]
  (fn [unit]
    (with-worktree repo {:name (str (sanitize unit) "-" (System/nanoTime)) :ref ref}
      (fn [dir]
        (let [base    (read-files dir (files-of unit))
              payload ((runner/coding-run {:session!   session!
                                           :base-files (constantly base)
                                           :task-of    (fn [x] x)})
                       unit)]
          (cond
            (nil? payload)                        nil
            (nil? test-cmd)                       payload
            (:green? (gate! dir payload test-cmd)) payload
            :else                                  nil))))))
