#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Build the C shim (fast) and then build Swift against the small module.
clang -c -O2 flintdb_swift_shim.c -o flintdb_swift_shim.o

swiftc \
  -I . \
  flintdb_swift_shim.o FlintDBSwift.swift tutorial.swift main.swift \
  -L ../../lib -lflintdb \
  -Xlinker -rpath -Xlinker ../../lib \
  -o tutorial

echo "Built: ./tutorial"
