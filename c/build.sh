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
# ./build.sh --win64 # to cross-compile for Windows x64
#
# How to run
# cd bin && ./flintdb 


set -e

# Parse command line options
BUILD_PLUGINS=0
ENABLE_MTRACE=0
CROSS_COMPILE_WIN64=0

for arg in "$@"; do
    case "$arg" in
        -all|--all)
            BUILD_PLUGINS=1
            ;;
        -mtrace|--mtrace)
            ENABLE_MTRACE=1
            ;;
        -win64|--win64)
            CROSS_COMPILE_WIN64=1
            ;;
    esac
done

MAKE_ARGS=""
if [ $CROSS_COMPILE_WIN64 -eq 1 ]; then
    echo "=== FlintDB Cross-Compile Build for Windows x64 ==="
    MAKE_ARGS="CROSS_COMPILE=win64"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # Auto-detect Homebrew mingw-w64 sysroot on macOS
        MINGW_PREFIX=$(brew --prefix mingw-w64 2>/dev/null)
        if [ -n "$MINGW_PREFIX" ]; then
            SYSROOT_PATH="$MINGW_PREFIX/x86_64-w64-mingw32"
            if [ -d "$SYSROOT_PATH" ]; then
                echo "Found mingw-w64 sysroot: $SYSROOT_PATH"
                MAKE_ARGS="$MAKE_ARGS MINGW_SYSROOT=$SYSROOT_PATH"
            fi
        fi
    fi
elif [ $BUILD_PLUGINS -eq 1 ]; then
    echo "=== FlintDB Plugin System Build (with plugins) ==="
elif [ $ENABLE_MTRACE -eq 1 ]; then
    echo "=== FlintDB Build (with memory tracing) ==="
else
    echo "=== FlintDB Core Build ==="
fi

# Check prerequisites
echo ""
echo "[1/3] Checking prerequisites..."
COMPILER_TO_CHECK=""
if [ $CROSS_COMPILE_WIN64 -eq 1 ]; then
    if ! command -v x86_64-w64-mingw32-gcc &>/dev/null; then
        echo "Error: Cross-compiler x86_64-w64-mingw32-gcc not found."
        echo "On macOS, install with: brew install mingw-w64"
        exit 1
    fi
    COMPILER_TO_CHECK="x86_64-w64-mingw32-gcc"
    echo "C compiler found: $(command -v $COMPILER_TO_CHECK)"
else
    if ! command -v gcc &>/dev/null && ! command -v clang &>/dev/null; then
        echo "Error: No C compiler found (gcc or clang required)"
        exit 1
    fi
    COMPILER_TO_CHECK=$(command -v gcc || command -v clang)
    echo "C compiler found: $COMPILER_TO_CHECK"
fi

if echo '#include <zlib.h>' | $COMPILER_TO_CHECK -E - &>/dev/null; then
    echo "zlib found for target"
else
    echo "Error: zlib.h development files not found for target compiler ($COMPILER_TO_CHECK)"
    if [ $CROSS_COMPILE_WIN64 -eq 1 ]; then
        echo "Ensure the mingw-w64 toolchain has zlib available."
    fi
    exit 1
fi
echo ""

# Detect make command (Linux/macOS: make; MSYS2/MinGW: mingw32-make)
MAKE_CMD=""
if command -v make &>/dev/null; then
    MAKE_CMD="make"
elif command -v gmake &>/dev/null; then
    MAKE_CMD="gmake"
elif command -v mingw32-make &>/dev/null; then
    MAKE_CMD="mingw32-make"
else
    echo "Error: make not found (make/gmake/mingw32-make required)"
    if [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* || "$OSTYPE" == "win32"* ]]; then
        echo "On MSYS2, install with: pacman -S --needed mingw-w64-x86_64-make"
    else
        echo "On Ubuntu/Debian, install with: sudo apt-get install build-essential"
        echo "On RHEL/Fedora, install with: sudo yum install make"
        echo "On macOS, install with: xcode-select --install"
    fi
    exit 1
fi
echo "Using make command: $MAKE_CMD"


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
"$MAKE_CMD" clean
if [ $ENABLE_MTRACE -eq 1 ]; then
    "$MAKE_CMD" BUILD_SO=1 ALLOCATOR=$ALLOCATOR CFLAGS="-DMTRACE=1" $MAKE_ARGS
else
    "$MAKE_CMD" BUILD_SO=1 ALLOCATOR=$ALLOCATOR NDEBUG=1 $MAKE_ARGS
fi
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
                if MAKE_CMD="$MAKE_CMD" bash build.sh; then
                    # Verify the plugin artifact actually landed in ../../lib
                    if ls "../../lib/libflintdb_${plugin_name}."* >/dev/null 2>&1 || ls "../../lib/libflintdb_${plugin_name}_"* >/dev/null 2>&1; then
                        INSTALLED_PLUGINS=$((INSTALLED_PLUGINS + 1))
                        echo "  ✓ $plugin_name built successfully"
                    else
                        echo "  ✗ Warning: $plugin_name build succeeded but no plugin library was installed into lib/"
                    fi
                else
                    echo "  ✗ Warning: $plugin_name build failed. Continuing..."
                fi
            elif [ -f "Makefile" ]; then
                if "$MAKE_CMD"; then
                    # Verify the plugin artifact actually landed in ../../lib
                    if ls "../../lib/libflintdb_${plugin_name}."* >/dev/null 2>&1 || ls "../../lib/libflintdb_${plugin_name}_"* >/dev/null 2>&1; then
                        INSTALLED_PLUGINS=$((INSTALLED_PLUGINS + 1))
                        echo "  ✓ $plugin_name built successfully"
                    else
                        echo "  ✗ Warning: $plugin_name build succeeded but no plugin library was installed into lib/"
                    fi
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