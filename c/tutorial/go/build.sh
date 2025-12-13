#!/bin/bash

# Build FlintDB Go Tutorial

cd "$(dirname "$0")"

echo "Building FlintDB Go tutorial..."
go clean -cache
CGO_LDFLAGS="-L$(pwd)/../../lib" go build -o tutorial .

if [ $? -eq 0 ]; then
    echo "✓ Build successful!"
    echo ""
    echo "Run with:"
    echo "  DYLD_LIBRARY_PATH=\$(pwd)/../../lib ./tutorial"
    echo ""
    echo "Or use:"
    echo "  export DYLD_LIBRARY_PATH=\$(pwd)/../../lib"
    echo "  ./tutorial"
else
    echo "✗ Build failed"
    exit 1
fi
