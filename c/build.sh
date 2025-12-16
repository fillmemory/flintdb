#!/bin/bash
#
# Build script for Linux, macOS, and Windows (MinGW)
#
# Prerequisites:
# Compiler: gcc, clang
#   Redhat: sudo yum install gcc make zlib-devel arrow-devel cjson-devel gperftools-devel jemalloc-devel
#   Ubuntu: sudo apt-get install build-essential zlib1g-dev libarrow-dev libcjson-dev libgoogle-perftools-dev libjemalloc-dev
#  Mingw64: pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-zlib mingw-w64-x86_64-arrow mingw-w64-x86_64-cjson mingw-w64-x86_64-gperftools mingw-w64-x86_64-jemalloc
#    macOS: brew install zlib apache-arrow cjson gperftools jemalloc 
#
# How to build 
# ./build.sh 
# ./build.sh -all   # to build with plugins
#
# How to run
# cd bin && ./flintdb 


set -e

# Check if -all option is provided
BUILD_PLUGINS=0
if [ "$1" = "-all" ]; then
    BUILD_PLUGINS=1
    echo "=== FlintDB Plugin System Build (with plugins) ==="
else
    echo "=== FlintDB Core Build ==="
fi

# Check prerequisites
echo ""
echo "[1/3] Checking prerequisites..."
if ! command -v gcc &>/dev/null && ! command -v clang &>/dev/null; then
    echo "Error: No C compiler found (gcc or clang required)"
    exit 1
fi
echo "C compiler found: $(command -v gcc || command -v clang)"

if echo '#include <zlib.h>' | gcc -E - &>/dev/null; then
    echo "zlib found"
else
    echo "Error: zlib development files not found"
    exit 1
fi
echo ""


# Detect available memory allocator
ALLOCATOR=none
AUTO_DETECT_ALLOCATOR=1

if [ $AUTO_DETECT_ALLOCATOR -eq 1 ]; then
    # Detect OS and check for memory allocators accordingly
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if brew list jemalloc &>/dev/null || [ -f /usr/local/lib/libjemalloc.dylib ] || [ -f /opt/homebrew/lib/libjemalloc.dylib ]; then
            ALLOCATOR=jemalloc
            echo "Using jemalloc as memory allocator"
        elif brew list gperftools &>/dev/null || [ -f /usr/local/lib/libtcmalloc.dylib ] || [ -f /opt/homebrew/lib/libtcmalloc.dylib ]; then
            ALLOCATOR=tcmalloc
            echo "Using tcmalloc as memory allocator"
        else
            echo "No specialized memory allocator found, using default"
        fi
    else
        if [ -f /usr/lib/libtcmalloc.so ] || [ -f /usr/lib/x86_64-linux-gnu/libtcmalloc.so ] || [ -f /usr/local/lib/libtcmalloc.so ]; then
            ALLOCATOR=tcmalloc
            echo "Using tcmalloc as memory allocator"
        elif [ -f /usr/lib/libjemalloc.so ] || [ -f /usr/lib/x86_64-linux-gnu/libjemalloc.so ] || [ -f /usr/local/lib/libjemalloc.so ]; then
            ALLOCATOR=jemalloc
            echo "Using jemalloc as memory allocator"
        else
            echo "No specialized memory allocator found, using default"
        fi
    fi
fi
echo ""

# 1. Build main FlintDB library
echo "[2/3] Building FlintDB core library..."
make clean
make BUILD_SO=1 ALLOCATOR=$ALLOCATOR NDEBUG=1
# make clean

if [ $BUILD_PLUGINS -eq 1 ]; then
    # 2. Build all plugins

    echo ""
    echo "[3/3] Building plugins..."
    PLUGIN_COUNT=0
    INSTALLED_PLUGINS=0

    # Iterate through all subdirectories in plugins/
    for plugin_dir in plugins/*/; do
        if [ -d "$plugin_dir" ]; then
            plugin_name=$(basename "$plugin_dir")
            echo "  Building plugin: $plugin_name"
            PLUGIN_COUNT=$((PLUGIN_COUNT + 1))
            
            cd "$plugin_dir"
            
            # Try different build methods
            if [ -f "build.sh" ]; then
                if bash build.sh; then
                    INSTALLED_PLUGINS=$((INSTALLED_PLUGINS + 1))
                    echo "  ✓ $plugin_name built successfully"
                else
                    echo "  ✗ Warning: $plugin_name build failed. Continuing..."
                fi
            elif [ -f "Makefile" ]; then
                if make; then
                    INSTALLED_PLUGINS=$((INSTALLED_PLUGINS + 1))
                    echo "  ✓ $plugin_name built successfully"
                else
                    echo "  ✗ Warning: $plugin_name build failed. Continuing..."
                fi
            else
                echo "  ⚠ No build.sh or Makefile found for $plugin_name"
            fi
            
            cd ../..
        fi
    done

    if [ $PLUGIN_COUNT -eq 0 ]; then
        echo "  No plugins found in plugins/ directory"
    fi

    # List installed plugins
    echo ""
    echo "=== Build Summary ==="
    echo "Plugins found: $PLUGIN_COUNT"
    echo "Plugins installed: $INSTALLED_PLUGINS"
    echo ""
else
    echo ""
    echo "[3/3] Skipping plugin build (use -all to build with plugins)"
fi

echo ""

# Clear screen if build was successful (no errors)
#clear

echo "=== Build Complete ==="
echo "Main library: lib/libflintdb.{so,dylib}"
echo "Executable: bin/flintdb"
# check successful, list built files
echo ""
# ls -l bin/flintdb lib/libflintdb.* lib/libflintdb_*.{so,dylib,dll} 2>/dev/null
if [[ "$OSTYPE" == "darwin"* ]]; then
  stat -f $'%z\t%Sm\t%N' -t "%Y-%m-%d %H:%M:%S" bin/* lib/* 2>/dev/null
else
  stat -c $'%s\t%y\t%n' bin/* lib/* 2>/dev/null \
    | awk -F'\t' '{ dt=$2; sub(/\.[0-9]+/,"",dt); sub(/ [+-][0-9:]+$/,"",dt); printf "%s\t%s\t%s\n",$1,dt,$3 }'
fi

if [ $BUILD_PLUGINS -eq 1 ]; then
    echo ""
    echo "To use plugins, ensure they are in the lib/ directory or set FLINTDB_PLUGIN_PATH environment variable."
fi
echo ""