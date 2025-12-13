#!/bin/bash

# Generate Rust bindings from C header using bindgen
echo "Generating Rust bindings from flintdb.h..."
BINDGEN="$HOME/.cargo/bin/bindgen"

if ! command -v bindgen &> /dev/null && [ ! -f "$BINDGEN" ]; then
    echo "bindgen not found, installing..."
    cargo install bindgen-cli
fi

# Use bindgen from cargo bin or system PATH
if [ -f "$BINDGEN" ]; then
    "$BINDGEN" ../../src/flintdb.h -o src/bindings.rs -- -I../../src
else
    bindgen ../../src/flintdb.h -o src/bindings.rs -- -I../../src
fi
echo "Bindings generated successfully."
echo ""

# Build the Rust tutorial
cargo build --release

# Run the tutorial with library path set
echo ""
echo "Running tutorial..."
DYLD_LIBRARY_PATH="../../lib" ./target/release/flintdb-tutorial
