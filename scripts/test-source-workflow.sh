#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/nac-source-workflow.XXXXXX")"
MAKE=${MAKE:-make}
cleanup() {
  rm -rf "$TMP"
}
trap cleanup EXIT

fail() {
  echo "test-source-workflow: $*" >&2
  exit 1
}

help="$($MAKE -C "$ROOT" help 2>/dev/null)"
for target in setup build dev run install-dev; do
  [[ "$help" == *"  $target"* ]] || fail "help omits $target"
done
if "$MAKE" -C "$ROOT" -n demo >"$TMP/demo.out" 2>&1; then
  fail "removed demo target is still callable"
fi
if rg -n --glob '!target-review-all13/**' --glob '!scripts/test-source-workflow.sh' 'make demo|^demo:' "$ROOT" >"$TMP/demo.matches"; then
  cat "$TMP/demo.matches" >&2
  fail "maintained source still references the removed demo target"
fi

source_binary="$TMP/source binary"
printf '%s\n' '#!/bin/sh' "printf '%s\\n' source-build" >"$source_binary"
chmod +x "$source_binary"
install_dir="$TMP/install path/with spaces"
"$ROOT/scripts/install-dev.sh" "$source_binary" "$install_dir" nac-my-branch
[[ -x "$install_dir/nac-my-branch" ]] || fail "custom binary was not installed"
[[ "$("$install_dir/nac-my-branch")" == source-build ]] || fail "installed binary changed"
for unsafe in '' nac-web .hidden '../escape' 'nested/name' 'white space'; do
  if "$ROOT/scripts/install-dev.sh" "$source_binary" "$install_dir" "$unsafe" >"$TMP/install.out" 2>&1; then
    fail "unsafe DEV_BIN_NAME was accepted: '$unsafe'"
  fi
done

fake_bin="$TMP/fake-bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/cargo" <<'EOF'
#!/bin/sh
exit "${FAKE_CARGO_STATUS:-0}"
EOF
cat >"$fake_bin/curl" <<'EOF'
#!/bin/sh
count_file=${FAKE_CURL_COUNT_FILE:?}
count=0
if [ -f "$count_file" ]; then count=$(cat "$count_file"); fi
count=$((count + 1))
printf '%s\n' "$count" >"$count_file"
# The first two probes verify that the ports are free. Later probes are ready.
[ "$count" -gt 2 ]
EOF
cat >"$fake_bin/npm" <<'EOF'
#!/bin/sh
if [ "${FAKE_VITE_FAIL:-0}" = 1 ]; then exit 23; fi
sh -c 'trap "exit 0" TERM INT; while :; do sleep 1; done' &
printf '%s\n' "$!" >"${FAKE_VITE_DESCENDANT:?}"
trap 'exit 0' TERM INT
while :; do sleep 1; done
EOF
cat >"$TMP/backend" <<'EOF'
#!/bin/sh
if [ "${FAKE_BACKEND_FAIL:-0}" = 1 ]; then exit 17; fi
sh -c 'trap "exit 0" TERM INT; while :; do sleep 1; done' &
printf '%s\n' "$!" >"${FAKE_BACKEND_DESCENDANT:?}"
trap 'exit 0' TERM INT
while :; do sleep 1; done
EOF
chmod +x "$fake_bin/cargo" "$fake_bin/curl" "$fake_bin/npm" "$TMP/backend"

run_supervisor() {
  local label=$1
  local curl_count="$TMP/$label-curl-count"
  FAKE_CURL_COUNT_FILE="$curl_count" \
  FAKE_BACKEND_DESCENDANT="$TMP/$label-backend-child" \
  FAKE_VITE_DESCENDANT="$TMP/$label-vite-child" \
  NAC_DEV_SERVER_BIN="$TMP/backend" \
  DEV_OPEN=0 PATH="$fake_bin:$PATH" \
    "$ROOT/start_dev.sh" >"$TMP/$label.log" 2>&1 &
  SUPERVISOR_PID=$!
}

run_supervisor interrupt
for _ in $(seq 1 100); do
  [[ -f "$TMP/interrupt-backend-child" && -f "$TMP/interrupt-vite-child" ]] && break
  kill -0 "$SUPERVISOR_PID" 2>/dev/null || fail "supervisor exited before becoming ready"
  sleep 0.05
done
[[ -f "$TMP/interrupt-backend-child" && -f "$TMP/interrupt-vite-child" ]] || fail "fake descendants were not started"
backend_child=$(cat "$TMP/interrupt-backend-child")
vite_child=$(cat "$TMP/interrupt-vite-child")
kill -TERM "$SUPERVISOR_PID"
if wait "$SUPERVISOR_PID"; then
  fail "TERM unexpectedly returned success"
else
  status=$?
fi
[[ "$status" -eq 143 ]] || fail "TERM returned $status instead of 143"
sleep 0.2
kill -0 "$backend_child" 2>/dev/null && fail "backend descendant survived cleanup"
kill -0 "$vite_child" 2>/dev/null && fail "Vite descendant survived cleanup"

FAKE_BACKEND_FAIL=1 run_supervisor backend-failure
if wait "$SUPERVISOR_PID"; then
  fail "backend startup failure unexpectedly succeeded"
else
  status=$?
fi
[[ "$status" -eq 17 ]] || fail "backend failure returned $status instead of 17"

echo "source workflow contracts passed"
