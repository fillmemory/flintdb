#!/usr/bin/env bash
set -euo pipefail

cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"/..

usage() {
  cat >&2 <<'EOF'
Usage:
  ./tools/repro_transaction_segv.sh [iters] [--asan|--ubsan] [--allocator <none|jemalloc|tcmalloc>] [--strpool <0|1>]

Examples:
  ./tools/repro_transaction_segv.sh 5000
  ./tools/repro_transaction_segv.sh 2000 --asan
  ./tools/repro_transaction_segv.sh 2000 --ubsan --allocator none
  ./tools/repro_transaction_segv.sh 5000 --allocator jemalloc --strpool 1

Notes:
  - ASan/UBSan builds use clang and are much slower.
  - With sanitizers, prefer --allocator none (jemalloc/tcmalloc may interfere).
EOF
}

ITERS="500"
SAN_MODE=""   # address|undefined
ALLOCATOR_OPT=""  # none|jemalloc|tcmalloc
STRPOOL_OPT=""    # 0|1

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --asan)
      SAN_MODE="address"
      shift
      ;;
    --ubsan)
      SAN_MODE="undefined"
      shift
      ;;
    --allocator)
      ALLOCATOR_OPT="${2:-}"
      shift 2
      ;;
    --strpool)
      STRPOOL_OPT="${2:-}"
      shift 2
      ;;
    *)
      # first positional = iterations
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        ITERS="$1"
        shift
      else
        echo "Unknown arg: $1" >&2
        usage
        exit 2
      fi
      ;;
  esac
done

# Plumb options into environment expected by testcase.sh
if [[ -n "$SAN_MODE" ]]; then
  export SANITIZE="$SAN_MODE"
  # Default allocator to none when sanitizing, unless user explicitly set it.
  if [[ -z "$ALLOCATOR_OPT" ]]; then
    ALLOCATOR_OPT="none"
  fi
fi
if [[ -n "$ALLOCATOR_OPT" ]]; then
  export ALLOCATOR="$ALLOCATOR_OPT"
fi
if [[ -n "$STRPOOL_OPT" ]]; then
  export VARIANT_STRPOOL="$STRPOOL_OPT"
fi

echo "[1/3] build (debug)" >&2
if [[ -n "${SANITIZE:-}" ]]; then
  echo "      SANITIZE=${SANITIZE} ALLOCATOR=${ALLOCATOR:-} VARIANT_STRPOOL=${VARIANT_STRPOOL:-}" >&2
else
  echo "      ALLOCATOR=${ALLOCATOR:-} VARIANT_STRPOOL=${VARIANT_STRPOOL:-}" >&2
fi
./testcase.sh TESTCASE_TRANSACTION --debug >/dev/null

if [[ ! -x ./testcase ]]; then
  echo "ERROR: ./testcase not found after build" >&2
  exit 2
fi

echo "[2/3] run loop: ${ITERS} iterations" >&2
for ((i=1; i<=ITERS; i++)); do
  if [[ $((i % 50)) -eq 0 ]]; then
    echo "  ... iteration ${i}" >&2
  fi

  # Keep stdout/stderr small unless we fail.
  ./testcase >/tmp/flintdb_testcase_last.log 2>&1 || {
    code=$?
    echo "[3/3] FAILED at iteration ${i} (exit=${code})" >&2
    tail -200 /tmp/flintdb_testcase_last.log >&2 || true

    echo "\n--- lldb bt all (same binary) ---" >&2
    lldb --batch -o "settings set target.process.stop-on-sharedlibrary-events false" \
         -o run \
         -o "bt all" \
         -o quit \
         -- ./testcase 2>&1 | tee /tmp/flintdb_lldb_bt.log >&2

    echo "\nSaved logs:" >&2
    echo "  /tmp/flintdb_testcase_last.log" >&2
    echo "  /tmp/flintdb_lldb_bt.log" >&2
    exit "$code"
  }

done

echo "OK: no crash in ${ITERS} iterations" >&2
