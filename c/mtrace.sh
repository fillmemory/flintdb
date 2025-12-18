#!/usr/bin/env bash

# mtrace.sh
# -------------
# This script recompiles the project with memory tracing enabled,
# runs a TPCH test case, and analyzes the memory trace log for leaks.
# Important: Makefile  -DMTRACE=1 must be set to enable mtrace
# Example: make CFLAGS="-DMTRACE=1" all

set -euo pipefail

# Always operate from the script's directory
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# Clean and recompile the project with mtrace enabled
make clean
make CFLAGS="-DMTRACE=1" all

# Compile the Java memory leak detector
mkdir -p temp/java
javac -d temp/java MemoryLeakDetector.java

# Run the TPCH test case with mtrace enabled
./testcase-cli/testcase_tpch.sh -wal TRUNCATE -limit 1000 2> temp/mtrace.log
java -cp temp/java MemoryLeakDetector temp/mtrace.log

