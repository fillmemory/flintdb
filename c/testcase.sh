#!/usr/bin/env bash
# Exit on error, undefined var, and fail on pipe errors
set -euo pipefail

# Always operate from the script's directory
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# ./testcase.sh TESTCASE_BUFFER --mtrace
# ./testcase.sh TESTCASE_STORAGE --mtrace
# ./testcase.sh TESTCASE_BPLUSTREE --mtrace
# ./testcase.sh TESTCASE_BPLUSTREE_DELETE --mtrace
# ./testcase.sh TESTCASE_BPLUSTREE_DELETE2 --mtrace
# ./testcase.sh TESTCASE_VARIANT --mtrace
# ./testcase.sh TESTCASE_SQL_PARSE --mtrace
# ./testcase.sh TESTCASE_SQL_META --mtrace
# ./testcase.sh TESTCASE_FORMATTER --mtrace
# ./testcase.sh TESTCASE_DECIMAL --mtrace
# ./testcase.sh TESTCASE_CSV_MULTILINE --mtrace
# ./testcase.sh TESTCASE_TABLE_BULK_INSERT --mtrace
# ./testcase.sh TESTCASE_TABLE_FIND --mtrace
# ./testcase.sh TESTCASE_TRANSACTION --mtrace
# ./testcase.sh TESTCASE_STREAM_GZIP_READ --mtrace
# ./testcase.sh TESTCASE_HYPERLOGLOG --mtrace
# ./testcase.sh TESTCASE_ROARINGBITMAP --mtrace
# ./testcase.sh TESTCASE_SORTABLE --mtrace
# ./testcase.sh TESTCASE_AGGREGATE_FUNCTIONS --mtrace
# ./testcase.sh TESTCASE_AGGREGATE_TUTORIAL --mtrace
# ./testcase.sh TESTCASE_MULTI_THREADS --mtrace
#
# ./testcase.sh TESTCASE_FLINTDB_TPCH_LINEITEM_WRITE --mtrace
# ./testcase.sh TESTCASE_FLINTDB_TPCH_LINEITEM_READ --mtrace
#
# Optional: switch allocator for A/B tests (link-time)
#   ALLOCATOR=jemalloc VARIANT_STRPOOL=1 STRPOOL_SIZE=1024 STRPOOL_CAPACITY=1024 ./testcase.sh TESTCASE_FLINTDB_TPCH_LINEITEM_WRITE --ndebug
#   ALLOCATOR=tcmalloc VARIANT_STRPOOL=1 STRPOOL_SIZE=1024 STRPOOL_CAPACITY=1024 ./testcase.sh TESTCASE_FLINTDB_TPCH_LINEITEM_WRITE --ndebug
#   ALLOCATOR=jemalloc ./testcase.sh TESTCASE_SQLITE_TPCH_LINEITEM_WRITE --ndebug

CASES=(
    TESTCASE_EXCEPTION
    TESTCASE_EXCEPTION2

    TESTCASE_BUFFER
    TESTCASE_STORAGE

    TESTCASE_BPLUSTREE
    TESTCASE_BPLUSTREE_DELETE2

    TESTCASE_VARIANT
    TESTCASE_SQL_PARSE

    TESTCASE_STRUCT_META
    TESTCASE_COLUMN_AT
    TESTCASE_SQL_META

    TESTCASE_FORMATTER
    TESTCASE_DECIMAL
    TESTCASE_CSV_MULTILINE
    TESTCASE_FILTER

    TESTCASE_TABLE_BULK_INSERT
    TESTCASE_TABLE_FIND
    TESTCASE_TRANSACTION

    TESTCASE_HYPERLOGLOG
    TESTCASE_ROARINGBITMAP

    TESTCASE_SORTABLE
    TESTCASE_AGGREGATE_FUNCTIONS
    TESTCASE_AGGREGATE_TUTORIAL
    TESTCASE_MULTI_THREADS

    TESTCASE_FLINTDB_TPCH_LINEITEM_WRITE
    TESTCASE_FLINTDB_TPCH_LINEITEM_READ

    TESTCASE_PERF_BUFIO_READ
    TESTCASE_PERF_TSV_READ
    TESTCASE_PERF_TSV_WRITE
    TESTCASE_PERF_STORAGE_WRITE
    TESTCASE_PERF_STORAGE_READ
    TESTCASE_PERF_BIN_ENCODE
    TESTCASE_PERF_BIN_DECODE
    TESTCASE_PERF_VARIANT_COMPARE
    TESTCASE_PERF_LRUCACHE

    TESTCASE_PARQUET_WRITE
    TESTCASE_PARQUET_READ
)


os="$(uname -s)"
case "${os}" in
Linux*)
	osname="linux"
	;;
Darwin*)
	osname="macos"
	;;
MINGW*|MSYS*)
	osname="win32"
	;;
*)    
	;;
esac

# clear

if [ $# -eq 0 ]; then
    echo "Usage: $0 <TESTCASE> [--mtrace]"
    echo "Available TESTCASE values:"
    for case in "${CASES[@]}"; do
        echo "  - $case"
    done
    exit 1
fi

TESTCASE=${1:-}
shift || true

# Parse flags after TESTCASE. Keep backward compatibility with the old
# "second-arg" style, but allow multiple flags in any order.
MTRACE=0
FLAG_DEBUG=0
FLAG_NDEBUG=0
FORWARD_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mtrace)
            MTRACE=1
            shift
            ;;
        --debug)
            FLAG_DEBUG=1
            shift
            ;;
        --ndebug)
            FLAG_NDEBUG=1
            shift
            ;;
        --)
            shift
            FORWARD_ARGS=("$@")
            break
            ;;
        *)
            # Unknown flag/arg: treat as executable argument (legacy)
            FORWARD_ARGS+=("$1")
            shift
            ;;
    esac
done

CC=gcc
STD=c23 # c99, c11, c17, c23

# Optional sanitizers (clang recommended):
#   SANITIZE=address   -> AddressSanitizer
#   SANITIZE=undefined -> UndefinedBehaviorSanitizer
# Notes:
# - Prefer ALLOCATOR=none with sanitizers (jemalloc/tcmalloc can interfere).
SANITIZE="${SANITIZE:-}"
if [[ -n "$SANITIZE" ]]; then
    # clang tends to have better sanitizer support on macOS.
    CC=clang
fi

# -DMTRACE for memory leak check with mtrace()
# -O0 -g for debug
# -O2 for release
# -O3 -march=native for optimized build
CFLAGS="-O3 -march=native -std=$STD -DUNIT_TEST -I/usr/local/include "
#LDFLAGS="-lc -lm -lpthread -lz -llz4 -lzstd -lsnappy -L/usr/local/lib "
LDFLAGS="-lc -lm -lpthread -lz -L/usr/local/lib "

# Apply sanitizers (must be set before compile).
case "$SANITIZE" in
    address)
        # Avoid -march=native with sanitizers for more predictable stack traces.
        CFLAGS="${CFLAGS/-march=native /} -fsanitize=address -fno-omit-frame-pointer"
        LDFLAGS="$LDFLAGS -fsanitize=address"
        ;;
    undefined)
        CFLAGS="${CFLAGS/-march=native /} -fsanitize=undefined -fno-omit-frame-pointer"
        LDFLAGS="$LDFLAGS -fsanitize=undefined"
        ;;
    "")
        ;;
    *)
        echo "Unknown SANITIZE=$SANITIZE (supported: address, undefined)" >&2
        exit 2
        ;;
esac

# Optional allocator selection: none|jemalloc|tcmalloc|auto
ALLOCATOR="${ALLOCATOR:-auto}"

if [[ "$ALLOCATOR" == "auto" ]]; then
    ALLOCATOR="none" # default

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
        # Linux
        if command -v ldconfig &>/dev/null; then
            if ldconfig -p | grep -q jemalloc; then
                ALLOCATOR=jemalloc
                echo "Using jemalloc as memory allocator"
            elif ldconfig -p | grep -q tcmalloc; then
                ALLOCATOR=tcmalloc
                echo "Using tcmalloc as memory allocator"
            else
                echo "No specialized memory allocator found, using default"
            fi
        else
            echo "No specialized memory allocator found, using default"
        fi
    fi
fi

if [ -d "/opt/homebrew/lib" ]; then
	CFLAGS="$CFLAGS -I/opt/homebrew/include"
	LDFLAGS="$LDFLAGS -L/opt/homebrew/lib"
fi

# Link against selected allocator if requested
case "$ALLOCATOR" in
    jemalloc)
        LDFLAGS="$LDFLAGS -ljemalloc"
        ;;
    tcmalloc)
        LDFLAGS="$LDFLAGS -ltcmalloc"
        ;;
    *) ;;
esac

if [[ $MTRACE -eq 1 ]]; then
    CFLAGS="$CFLAGS -DMTRACE"
fi

if [[ $FLAG_NDEBUG -eq 1 ]]; then
    CFLAGS="$CFLAGS -DNDEBUG"
fi

# Debug build (useful for lldb). By default keeps assertions enabled unless --ndebug is also set.
if [[ $FLAG_DEBUG -eq 1 ]]; then
    CFLAGS="-O0 -g -std=$STD -DUNIT_TEST -I/usr/local/include "
    # Re-apply optional sanitizers and other flags that were appended earlier.
    # (SANITIZE block above runs before this and edits CFLAGS/LDFLAGS; we keep it by re-appending.)
    if [[ $MTRACE -eq 1 ]]; then
        CFLAGS="$CFLAGS -DMTRACE"
    fi
    if [[ $FLAG_NDEBUG -eq 1 ]]; then
        CFLAGS="$CFLAGS -DNDEBUG"
    fi

    # If sanitizers are enabled, re-apply them because CFLAGS was reset above.
    case "$SANITIZE" in
        address)
            CFLAGS="$CFLAGS -fsanitize=address -fno-omit-frame-pointer"
            if [[ "$LDFLAGS" != *"-fsanitize=address"* ]]; then
                LDFLAGS="$LDFLAGS -fsanitize=address"
            fi
            ;;
        undefined)
            CFLAGS="$CFLAGS -fsanitize=undefined -fno-omit-frame-pointer"
            if [[ "$LDFLAGS" != *"-fsanitize=undefined"* ]]; then
                LDFLAGS="$LDFLAGS -fsanitize=undefined"
            fi
            ;;
        "")
            ;;
    esac
    # Re-apply VARIANT_STRPOOL defines (added later) as usual.
fi

# Optional: enable small-string pooling used in src/variant.c
# Toggle via environment variables (same knobs as Makefile):
#  VARIANT_STRPOOL=1 STRPOOL_SIZE=1024 STRPOOL_CAPACITY=1024 ./testcase.sh TESTCASE_...
VARIANT_STRPOOL="${VARIANT_STRPOOL:-1}"
STRPOOL_SIZE="${STRPOOL_SIZE:-1024}"
STRPOOL_CAPACITY="${STRPOOL_CAPACITY:-1024}"
case "$VARIANT_STRPOOL" in
    1|yes|true|on)
        CFLAGS="$CFLAGS -DVARIANT_USE_STRPOOL=1 -DVARIANT_STRPOOL_CAPACITY=${STRPOOL_CAPACITY} -DVARIANT_STRPOOL_STR_SIZE=${STRPOOL_SIZE}"
        ;;
    *) ;;
esac
# CFLAGS="$CFLAGS -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE=1" # for large file support

EXE_NAME=./testcase
EXE_PATH=$EXE_NAME
#SRC_FILES=./src/*.c 
# all .c files in src/ but excluding cli.c
SRC_FILES=$(find ./src -maxdepth 1 -name '*.c' ! -name 'cli.c' | tr '\n' ' ')
# echo $SRC_FILES


target="$osname"
echo "compile" "$TESTCASE"


case "${target}" in
linux*)
    # Link sqlite3 only for sqlite-related testcase to avoid unnecessary dependency
    if [[ "$TESTCASE" == "TESTCASE_SQLITE_TPCH_LINEITEM_WRITE" ]]; then
        LDFLAGS="$LDFLAGS -lsqlite3"
    fi
    $CC $CFLAGS -D_GNU_SOURCE=1 -Dlinux -D"$TESTCASE" -o "$EXE_PATH" $SRC_FILES $LDFLAGS
	;;
macos*)
	#brew install zlib lz4 zstd snappy 
    if [[ "$TESTCASE" == "TESTCASE_SQLITE_TPCH_LINEITEM_WRITE" ]]; then
        LDFLAGS="$LDFLAGS -lsqlite3"
    fi
    $CC $CFLAGS -D"$TESTCASE" -o "$EXE_PATH" $SRC_FILES $LDFLAGS -DJOURNAL_MODE_WAL=1
	;;
win32*)
	# pacman -S zlib-devel liblz4-devel libzstd-devel
	# git checkout https://github.com/google/snappy.git && mkdir build && cd build && cmake ../ && make

    # Link Windows sockets libraries and correct library paths
    LDFLAGS="$LDFLAGS -lws2_32 -lwsock32 -L/usr/local/lib -L/mingw64/lib"
    if [[ "$TESTCASE" == "TESTCASE_SQLITE_TPCH_LINEITEM_WRITE" ]]; then
        LDFLAGS="$LDFLAGS -lsqlite3"
    fi
    $CC $CFLAGS -D_GNU_SOURCE=1 -D_WIN32 -DPATH_MAX=4096 -D"$TESTCASE" -o "$EXE_PATH" $SRC_FILES $LDFLAGS
	;;
*)
	echo "no target ${target}"
	;;
esac


if [ ! -f "$EXE_PATH" ]; then
    exit 1
fi

echo "run ..." "$TESTCASE"

if [[ $MTRACE -eq 1 ]]; then
    echo "Memory leak check with mtrace()"
    "$EXE_NAME" 2> temp/mtrace.log

    mkdir -p temp/java
    javac -d temp/java MemoryLeakDetector.java
    java -cp temp/java MemoryLeakDetector temp/mtrace.log
else
    # Forward any extra arguments (after TESTCASE and optional --mtrace) to the test executable
    # Usage examples:
    #   ./testcase.sh TESTCASE_PERF_LRUCACHE           # defaults
    #   ./testcase.sh TESTCASE_PERF_LRUCACHE -- 1000000 1000000 0 1048576 1048576
    # The double-dash (--) prevents bash from treating numbers as options.
    if [[ ${#FORWARD_ARGS[@]} -gt 0 ]]; then
        "$EXE_NAME" "${FORWARD_ARGS[@]}"
    else
        "$EXE_NAME"
    fi
fi

# rm -f "$EXE_NAME"
