#!/bin/bash

set -e

echo "Building FlintDB JsonL Plugin..."

# Check if Apache Arrow is installed
if command -v pkg-config &> /dev/null; then
    if pkg-config --exists arrow; then
        echo "Found Arrow via pkg-config"
        export ARROW_HOME=$(pkg-config --variable=prefix arrow)
    fi
fi

# Try homebrew on macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    if command -v brew &> /dev/null; then
        ARROW_PREFIX=$(brew --prefix apache-arrow 2>/dev/null || echo "")
        if [ -n "$ARROW_PREFIX" ]; then
            echo "Found Arrow via Homebrew: $ARROW_PREFIX"
            export ARROW_HOME=$ARROW_PREFIX
        fi
    fi
fi

# Build the plugin with adapter
make clean

# Build plugin library
make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)

# Copy plugin to lib directory
mkdir -p ../../lib
if [[ "$OSTYPE" == "darwin"* ]]; then
    cp libflintdb_*.dylib ../../lib/ 2>/dev/null || true
else
    cp libflintdb_*.so ../../lib/ 2>/dev/null || true
fi

make clean

echo ""
echo "Build successful!"
echo ""
echo "Plugin location:"
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "  $(pwd)/../../lib/libflintdb_jsonl.dylib"
else
    echo "  $(pwd)/../../lib/libflintdb_jsonl.so"
fi
echo ""
echo "The plugin will be automatically loaded by flintdb when accessing .jsonl files"
