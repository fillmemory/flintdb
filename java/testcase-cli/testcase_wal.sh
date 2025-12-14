#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)

cd "${ROOT_DIR}"

run() {
    echo "+ $*"
    "$@"
}

run_with_timeout() {
    local seconds="$1"
    shift
    python3 - <<'PY' "$seconds" "$@"
import subprocess, sys
timeout = float(sys.argv[1])
cmd = sys.argv[2:]
try:
    subprocess.run(cmd, check=True, timeout=timeout)
except subprocess.TimeoutExpired:
    print(f"ASSERT FAIL: command timed out after {timeout}s: {' '.join(cmd)}", file=sys.stderr)
    sys.exit(1)
PY
}

assert_file_exists() {
    local f="$1"
    if [[ ! -f "$f" ]]; then
        echo "ASSERT FAIL: missing file: $f" >&2
        exit 1
    fi
}

assert_eq() {
    local got="$1"
    local expected="$2"
    local msg="${3:-}"
    if [[ "$got" != "$expected" ]]; then
        echo "ASSERT FAIL: expected=$expected got=$got ${msg}" >&2
        exit 1
    fi
}

assert_ge() {
    local got="$1"
    local expected="$2"
    local msg="${3:-}"
    if (( got < expected )); then
        echo "ASSERT FAIL: expected >= $expected got=$got ${msg}" >&2
        exit 1
    fi
}

# Read (committedOffset, checkpointOffset) from WAL header.
# Header layout (little-endian):
#   0..3 magic, 4..5 ver, 6..7 headerSize, 8..15 timestamp
#   16..23 txId, 24..31 committedOffset, 32..39 checkpointOffset
read_wal_offsets() {
    local wal="$1"
    python3 - <<'PY' "$wal"
import struct, sys
path = sys.argv[1]
with open(path, 'rb') as f:
        header = f.read(4096)
if len(header) < 40:
        print("0 0")
        sys.exit(0)
committed = struct.unpack_from('<q', header, 24)[0]
checkpoint = struct.unpack_from('<q', header, 32)[0]
print(f"{committed} {checkpoint}")
PY
}

# Keep recovery tests deterministic: include page images so replay is possible
export FLINTDB_WAL_PAGE_DATA=1

ORIG_WAL_CHECKPOINT_INTERVAL="${FLINTDB_WAL_CHECKPOINT_INTERVAL-}"

echo "========================================="
echo "Test 1: WAL=TRUNCATE (auto-truncate mode)"
echo "========================================="

# For TRUNCATE-mode tests, force frequent checkpoints so truncation is observable even in short-lived CLI runs.
export FLINTDB_WAL_CHECKPOINT_INTERVAL=1

SQL_CREATE_TABLE=$(cat <<- EOF
CREATE TABLE temp/testcase.flintdb (
    id UINT,
    name STRING(100),
    age UINT8,
    salary DECIMAL(10,2),

    PRIMARY KEY (id)
) WAL=TRUNCATE
EOF
)

echo "DROP TABLE"
run ./bin/flintdb "DROP TABLE temp/testcase.flintdb" -log || true
if [ -f temp/testcase.flintdb ]; then
    echo "Removing existing temp/testcase.flintdb"
    rm temp/testcase.flintdb temp/testcase.flintdb.*
fi


echo "CREATE TABLE"
run ./bin/flintdb "$SQL_CREATE_TABLE" -log -status


echo "INSERT INTO (with column specification)"
run ./bin/flintdb "INSERT INTO temp/testcase.flintdb (id, name, age, salary) VALUES (1, 'Alice', 30, 60000.00)" -log
run ./bin/flintdb "INSERT INTO temp/testcase.flintdb (id, name, age, salary) VALUES (2, 'Bob', 25, 50000.00)" -log

echo "INSERT INTO (without column specification - all columns in order)"
run ./bin/flintdb "INSERT INTO temp/testcase.flintdb VALUES (3, 'Charlie', 35, 70000.00)" -log


echo "UPDATE"
run ./bin/flintdb "UPDATE temp/testcase.flintdb SET salary = 65000.00 WHERE id = 2" -log

echo "DELETE"
run ./bin/flintdb "DELETE FROM temp/testcase.flintdb WHERE id = 2" -log

echo ""
echo "Validate TRUNCATE mode WAL header offsets"
assert_file_exists temp/testcase.flintdb.wal
WAL_SZ=$(stat -f%z temp/testcase.flintdb.wal)
# TRUNCATE mode now checkpoints+truncates on close; WAL should be header-only.
assert_eq "$WAL_SZ" "4096" "(WAL size should be header-only in TRUNCATE mode)"
read COMMITTED CHECKPOINT < <(read_wal_offsets temp/testcase.flintdb.wal)
assert_eq "$COMMITTED" "4096" "(committedOffset)"
assert_eq "$CHECKPOINT" "4096" "(checkpointOffset)"

echo ""
echo "WAL file size after operations:"
if [ -f temp/testcase.flintdb.wal ]; then
    ls -lh temp/testcase.flintdb.wal
else
    echo "WAL file not found (may have been truncated)"
fi

# Restore checkpoint interval for subsequent tests
if [[ -n "${ORIG_WAL_CHECKPOINT_INTERVAL}" ]]; then
    export FLINTDB_WAL_CHECKPOINT_INTERVAL="${ORIG_WAL_CHECKPOINT_INTERVAL}"
else
    unset FLINTDB_WAL_CHECKPOINT_INTERVAL
fi

echo ""
echo "========================================="
echo "Test 2: WAL=LOG (keep all logs mode)"
echo "========================================="

SQL_CREATE_TABLE_LOG=$(cat <<- EOF
CREATE TABLE temp/testcase_log.flintdb (
    id UINT,
    name STRING(100),
    age UINT8,
    salary DECIMAL(10,2),

    PRIMARY KEY (id)
) WAL=LOG
EOF
)

echo "DROP TABLE"
run ./bin/flintdb "DROP TABLE temp/testcase_log.flintdb" -log || true
if [ -f temp/testcase_log.flintdb ]; then
    rm temp/testcase_log.flintdb temp/testcase_log.flintdb.*
fi

echo "CREATE TABLE (LOG mode)"
run ./bin/flintdb "$SQL_CREATE_TABLE_LOG" -log

echo "INSERT operations"
run ./bin/flintdb "INSERT INTO temp/testcase_log.flintdb VALUES (1, 'Test1', 20, 40000.00)" -log
run ./bin/flintdb "INSERT INTO temp/testcase_log.flintdb VALUES (2, 'Test2', 30, 50000.00)" -log

echo "Generate more WAL records (LOG mode)"
for i in $(seq 1 200); do
    if (( i % 2 == 0 )); then
        run ./bin/flintdb "UPDATE temp/testcase_log.flintdb SET salary = 40000.00 WHERE id = 1" -log >/dev/null
    else
        run ./bin/flintdb "UPDATE temp/testcase_log.flintdb SET salary = 40000.01 WHERE id = 1" -log >/dev/null
    fi
done

echo "Validate LOG mode WAL header offsets (committedOffset should advance)"
assert_file_exists temp/testcase_log.flintdb.wal
WAL_SZ_LOG=$(stat -f%z temp/testcase_log.flintdb.wal)
assert_ge "$WAL_SZ_LOG" 4096 "(WAL file should be >= header)"
read COMMITTED_LOG CHECKPOINT_LOG < <(read_wal_offsets temp/testcase_log.flintdb.wal)
assert_ge "$COMMITTED_LOG" 4096 "(committedOffset should be >= header)"
# In LOG mode we may not checkpoint; checkpointOffset can legally be 0.

echo "Corrupt WAL tail and ensure recovery stops safely"
cp temp/testcase_log.flintdb.wal temp/testcase_log.flintdb.wal.bak
dd if=/dev/urandom bs=1 count=17 >> temp/testcase_log.flintdb.wal 2>/dev/null
run ./bin/flintdb "SELECT COUNT(*) FROM temp/testcase_log.flintdb" -log >/dev/null
mv temp/testcase_log.flintdb.wal.bak temp/testcase_log.flintdb.wal

echo ""
echo "WAL file size with LOG mode:"
if [ -f temp/testcase_log.flintdb.wal ]; then
    ls -lh temp/testcase_log.flintdb.wal
fi

echo ""
echo "========================================="
echo "Test 2b: Hang-like case (padded WAL tail)"
echo "========================================="

echo "Pad WAL with zeros beyond committedOffset (simulates preallocation / tail garbage)"
assert_file_exists temp/testcase_log.flintdb.wal
cp temp/testcase_log.flintdb.wal temp/testcase_log.flintdb.wal.pad.bak

# Add a sizable zero tail. If recovery scans to EOF, it can look like a hang.
dd if=/dev/zero bs=1m count=64 >> temp/testcase_log.flintdb.wal 2>/dev/null

echo "Run a simple read query with timeout (should not hang)"
run_with_timeout 15 ./bin/flintdb "SELECT COUNT(*) FROM temp/testcase_log.flintdb" -log >/dev/null

mv temp/testcase_log.flintdb.wal.pad.bak temp/testcase_log.flintdb.wal

echo ""
echo "========================================="
echo "Test 3: WAL=OFF (disabled mode)"
echo "========================================="



SQL_CREATE_TABLE_OFF=$(cat <<- EOF
CREATE TABLE temp/testcase_none.flintdb (
    id UINT,
    name STRING(100),

    PRIMARY KEY (id)
) WAL=OFF
EOF
)

echo "DROP TABLE"
run ./bin/flintdb "DROP TABLE temp/testcase_none.flintdb" -log || true
if [ -f temp/testcase_none.flintdb ]; then
    rm temp/testcase_none.flintdb temp/testcase_none.flintdb.*
fi

echo "CREATE TABLE (OFF mode)"
run ./bin/flintdb "$SQL_CREATE_TABLE_OFF" -log
run ./bin/flintdb "INSERT INTO temp/testcase_none.flintdb VALUES (1, 'NoWAL')" -log

echo ""
echo "WAL file should not exist:"
if [ -f temp/testcase_none.flintdb.wal ]; then
    echo "ERROR: WAL file exists when it shouldn't"
    ls -lh temp/testcase_none.flintdb.wal
else
    echo "OK: No WAL file (as expected)"
fi

echo ""
echo "All WAL tests passed."

