#!/usr/bin/env bash
# install-node.sh — install a resident fleet node as a LaunchAgent (macOS) or
# systemd user timer (Linux). One firing = one bounded fleet.node round; the
# service relaunches on an interval, all state in the shared log.
#
# usage:
#   deploy/install-node.sh --role agent --graph k51q… --agents pc1-a1,pc1-a2 \
#       [--node pc1] [--interval 60] [--budget 20] [--or-key $OR_KEY] \
#       [--repo <path>] [--logdir <path>] [--dry-run] [--uninstall]
#
# --dry-run  render the unit and print it (no install) — safe to run anywhere.
# --uninstall  unload + remove the LaunchAgent / disable the timer.
set -euo pipefail

ROLE=agent NODE="$(hostname -s 2>/dev/null || echo node)" AGENTS="a1,a2"
GRAPH="" INTERVAL=60 BUDGET=20 OR_KEY="${OR_KEY:-}" DRY=0 UNINSTALL=0
HERE="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$HERE" LOGDIR="${TMPDIR:-/tmp}"

while [ $# -gt 0 ]; do
  case "$1" in
    --role) ROLE="$2"; shift 2;; --node) NODE="$2"; shift 2;;
    --agents) AGENTS="$2"; shift 2;; --graph) GRAPH="$2"; shift 2;;
    --interval) INTERVAL="$2"; shift 2;; --budget) BUDGET="$2"; shift 2;;
    --or-key) OR_KEY="$2"; shift 2;; --repo) REPO="$2"; shift 2;;
    --logdir) LOGDIR="$2"; shift 2;; --dry-run) DRY=1; shift;;
    --uninstall) UNINSTALL=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

case "$ROLE" in agent|governor|both) :;; *) echo "role must be agent|governor|both" >&2; exit 2;; esac

render_launchd() {
  sed -e "s#@NODE@#${NODE}#g" -e "s#@ROLE@#${ROLE}#g" -e "s#@AGENTS@#${AGENTS}#g" \
      -e "s#@BUDGET@#${BUDGET}#g" -e "s#@GRAPH@#${GRAPH}#g" -e "s#@OR_KEY@#${OR_KEY}#g" \
      -e "s#@INTERVAL@#${INTERVAL}#g" -e "s#@REPO@#${REPO}#g" -e "s#@LOGDIR@#${LOGDIR}#g" \
      "$HERE/deploy/launchd/net.kotoba-lang.fleet-node.plist.tmpl"
}

OS="$(uname -s)"
if [ "$OS" = "Darwin" ]; then
  LABEL="net.kotoba-lang.fleet-node.${NODE}"
  PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
  if [ "$UNINSTALL" = 1 ]; then
    launchctl unload "$PLIST" 2>/dev/null || true; rm -f "$PLIST"
    echo "uninstalled ${LABEL}"; exit 0
  fi
  if [ "$DRY" = 1 ]; then echo "# would install: $PLIST"; render_launchd; exit 0; fi
  mkdir -p "$(dirname "$PLIST")" "$LOGDIR"
  render_launchd > "$PLIST"
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load "$PLIST"
  echo "installed + loaded ${LABEL} (role=${ROLE} interval=${INTERVAL}s)"
else
  # Linux: env file + user timer
  ENVDIR="$HOME/.config/fleet-node"; INST="fleet-node@${NODE}"
  if [ "$UNINSTALL" = 1 ]; then
    systemctl --user disable --now "${INST}.timer" 2>/dev/null || true
    rm -f "$ENVDIR/${NODE}.env"; echo "uninstalled ${INST}"; exit 0
  fi
  ENVBODY="FLEET_NODE=${NODE}
FLEET_ROLE=${ROLE}
FLEET_AGENTS=${AGENTS}
FLEET_BUDGET=${BUDGET}
FLEET_GRAPH=${GRAPH}
OR_KEY=${OR_KEY}"
  if [ "$DRY" = 1 ]; then
    echo "# would write ${ENVDIR}/${NODE}.env:"; echo "$ENVBODY"
    echo "# and enable systemd --user timer ${INST}.timer (OnUnitActiveSec≈${INTERVAL}s)"; exit 0
  fi
  mkdir -p "$ENVDIR"; printf '%s\n' "$ENVBODY" > "$ENVDIR/${NODE}.env"
  systemctl --user enable --now "${INST}.timer"
  echo "installed + enabled ${INST}.timer (role=${ROLE})"
fi
