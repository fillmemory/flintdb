#!/bin/bash

# Run FlintDB Go Tutorial

cd "$(dirname "$0")"

export DYLD_LIBRARY_PATH="$(pwd)/../../lib:$DYLD_LIBRARY_PATH"

./tutorial
