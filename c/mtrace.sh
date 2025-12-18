#!/usr/bin/env bash

# Important: Makefile  -DMTRACE=1 must be set to enable mtrace
# Example: make CFLAGS="-DMTRACE=1" all

set -euo pipefail

# Always operate from the script's directory
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"


mkdir -p temp/java
javac -d temp/java MemoryLeakDetector.java

./testcase-cli/testcase_tpch.sh -wal TRUNCATE -limit 1000 2> temp/mtrace.log
java -cp temp/java MemoryLeakDetector temp/mtrace.log

