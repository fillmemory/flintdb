#!/usr/bin/env bash
# Exit on error, undefined var, and fail on pipe errors
set -euo pipefail

# Always operate from the script's directory
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"


mkdir -p temp/java
javac -d temp/java MemoryLeakDetector.java

./testcase-cli/testcase_tpch.sh -wal TRUNCATE -limit 1000 2> temp/mtrace.log
java -cp temp/java MemoryLeakDetector temp/mtrace.log

