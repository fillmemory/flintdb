#!/bin/bash

set -e

echo "Building FlintDB Parquet Plugin..."

MAKE_BIN="${MAKE_CMD:-make}"

# Check if Apache Arrow is installed
if command -v pkg-config &> /dev/null; then
    if pkg-config --exists arrow; then
        echo "Found Arrow via pkg-config"
        ARROW_PREFIX=$(pkg-config --variable=prefix arrow)
        if [ -d "$ARROW_PREFIX/include/arrow" ]; then
            export ARROW_HOME="$ARROW_PREFIX"
        else
            echo "Warning: pkg-config prefix does not contain include/arrow: $ARROW_PREFIX"
        fi
    fi
fi

# Try homebrew on macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    if command -v brew &> /dev/null; then
        ARROW_PREFIX=$(brew --prefix apache-arrow 2>/dev/null || echo "")
        if [ -n "$ARROW_PREFIX" ]; then
            if [ -d "$ARROW_PREFIX/include/arrow" ]; then
                echo "Found Arrow via Homebrew: $ARROW_PREFIX"
                export ARROW_HOME="$ARROW_PREFIX"
            else
                echo "Warning: Homebrew prefix does not contain include/arrow: $ARROW_PREFIX"
            fi
        fi
    fi
fi

# Check if ARROW_HOME is set
if [ -z "$ARROW_HOME" ]; then
    echo "Warning: ARROW_HOME not set, will try system paths"
    echo "Install Apache Arrow:"
    echo "  macOS: brew install apache-arrow"
    echo "  Linux: sudo apt-get install libarrow-dev libparquet-dev (Arrow typically installs under /usr)"
    echo "  Tip: ensure pkg-config is installed and can find Arrow (pkg-config --exists arrow)"
fi

# Test Arrow installation
"$MAKE_BIN" test-arrow || {
    echo ""
    echo "ERROR: Apache Arrow not found or not properly installed"
    echo ""
    echo "Please install Apache Arrow:"
    echo "  macOS: brew install apache-arrow"
    echo "  Linux: sudo apt-get install libarrow-dev libparquet-dev"
    echo ""
    echo "Or set ARROW_HOME environment variable:"
    echo "  export ARROW_HOME=/path/to/arrow"
    echo ""
    exit 1
}

# Build the plugin with adapter
"$MAKE_BIN" clean

# Build plugin library
"$MAKE_BIN" -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)

# Copy plugin to lib directory
mkdir -p ../../lib

# Windows/MSYS2: plugins are typically built as .dll (sometimes .so under POSIX layers)
UNAME_S=$(uname -s 2>/dev/null || echo "")
if [[ "$OSTYPE" == "darwin"* ]]; then
    cp libflintdb_parquet.dylib ../../lib/ 2>/dev/null || true
elif [[ "$UNAME_S" == *"_NT-"* ]] || [[ "$OSTYPE" == "msys"* ]] || [[ "$OSTYPE" == "cygwin"* ]]; then
    cp libflintdb_parquet.dll ../../lib/ 2>/dev/null || true
    cp libflintdb_parquet.so ../../lib/ 2>/dev/null || true
else
    cp libflintdb_parquet.so ../../lib/ 2>/dev/null || true
fi

"$MAKE_BIN" clean

echo ""
echo "Build successful!"
echo ""
echo "Plugin location:"
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "  $(pwd)/../../lib/libflintdb_parquet.dylib"
else
    echo "  $(pwd)/../../lib/libflintdb_parquet.so"
fi
echo ""
echo "The plugin will be automatically loaded by FlintDB when accessing .parquet files"
