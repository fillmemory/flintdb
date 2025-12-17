#!/bin/bash
#
# iOS build script for FlintDB (C core library)
#
# Outputs:
#   c/build/ios/FlintDB.xcframework
#
# Notes:
# - iOS does not support the CLI/WebUI parts (e.g. fork/open-browser), so this
#   script builds the core library only.
# - Requires Xcode command line tools.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT_DIR/src"
OUT_DIR="$ROOT_DIR/build/ios"

# Output base name (without extension)
LIB_NAME=${LIB_NAME:-"libflintdb_ios"}

if ! command -v xcrun >/dev/null 2>&1; then
  echo "Error: xcrun not found. Install Xcode Command Line Tools: xcode-select --install" >&2
  exit 1
fi
if ! command -v xcodebuild >/dev/null 2>&1; then
  echo "Error: xcodebuild not found. Install Xcode (or Command Line Tools)." >&2
  exit 1
fi

# Public headers to package inside the XCFramework
HDR_DIR="$OUT_DIR/headers"

# Exclude components that are not useful/valid on iOS builds.
# - cli/webui: iOS doesn't support fork/open-browser and we don't ship executables
# - runtime_win32: Windows-only compatibility layer
# - testcase*: test harness sources are not part of the library surface
EXCLUDE_REGEX='/(cli|webui|runtime_win32|testcase|testcase_filter)\.c$'

SOURCES=()
while IFS= read -r src; do
  SOURCES+=("$src")
done < <(find "$SRC_DIR" -maxdepth 1 -name "*.c" | LC_ALL=C sort | grep -Ev "$EXCLUDE_REGEX")

if [ ${#SOURCES[@]} -eq 0 ]; then
  echo "Error: no sources found under $SRC_DIR" >&2
  exit 1
fi

IOS_MIN_VERSION=${IOS_MIN_VERSION:-"13.0"}

DEVICE_ARCHS=${DEVICE_ARCHS:-"arm64"}
SIM_ARCHS=${SIM_ARCHS:-"arm64 x86_64"}

COMMON_CFLAGS=(
  -O3
  -DNDEBUG
  -D_GNU_SOURCE
  -I"$ROOT_DIR"
  -I"$SRC_DIR"
  -fvisibility=hidden
  -ffunction-sections
  -fdata-sections
)

build_one() {
  local sdk="$1"; shift
  local arch="$1"; shift
  local minflag="$1"; shift

  local cc
  cc="$(xcrun --sdk "$sdk" --find clang)"

  local sysroot
  sysroot="$(xcrun --sdk "$sdk" --show-sdk-path)"

  local build_dir="$OUT_DIR/$sdk-$arch"
  local obj_dir="$build_dir/obj"
  local lib_path="$build_dir/${LIB_NAME}.a"
  local dylib_path="$build_dir/${LIB_NAME}.dylib"

  rm -rf "$build_dir"
  mkdir -p "$obj_dir"

  local cflags=("${COMMON_CFLAGS[@]}" -isysroot "$sysroot" -arch "$arch" "$minflag" -std=gnu11)

  echo "==> Building $sdk ($arch)"
  for src in "${SOURCES[@]}"; do
    local base
    base="$(basename "$src" .c)"
    "$cc" "${cflags[@]}" -c "$src" -o "$obj_dir/$base.o"
  done

  # Use Apple's libtool to create a deterministic static library.
  xcrun --sdk "$sdk" libtool -static -o "$lib_path" "$obj_dir"/*.o
  echo "    -> $lib_path"

  # Build a dynamic library as well (useful for advanced embedding scenarios).
  # Note: on iOS, dylibs must be embedded within an app/framework; this is not a standalone install.
  local ldflags=("-dynamiclib" "-isysroot" "$sysroot" "-arch" "$arch" "$minflag" "-Wl,-dead_strip" "-Wl,-install_name,@rpath/${LIB_NAME}.dylib")
  "$cc" "${ldflags[@]}" -o "$dylib_path" "$obj_dir"/*.o
  echo "    -> $dylib_path"
}

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

# Headers packaged into the XCFramework
mkdir -p "$HDR_DIR"
cp -f "$SRC_DIR/flintdb.h" "$HDR_DIR/"
cp -f "$SRC_DIR/types.h" "$HDR_DIR/"

# Build device slices
for arch in $DEVICE_ARCHS; do
  build_one "iphoneos" "$arch" "-miphoneos-version-min=$IOS_MIN_VERSION"
done

# Build simulator slices
for arch in $SIM_ARCHS; do
  build_one "iphonesimulator" "$arch" "-mios-simulator-version-min=$IOS_MIN_VERSION"
done

# Create a single XCFramework with the first lib per SDK.
# If multiple archs were built per SDK, merge them with lipo.
merge_sdk_lib() {
  local sdk="$1"; shift
  local archs="$1"; shift
  local merged_dir="$OUT_DIR/$sdk-merged"
  mkdir -p "$merged_dir"

  local inputs=()
  for arch in $archs; do
    inputs+=("$OUT_DIR/$sdk-$arch/${LIB_NAME}.a")
  done

  if [ ${#inputs[@]} -eq 1 ]; then
    cp -f "${inputs[0]}" "$merged_dir/${LIB_NAME}.a"
  else
    xcrun lipo -create "${inputs[@]}" -output "$merged_dir/${LIB_NAME}.a"
  fi

  echo "$merged_dir/${LIB_NAME}.a"
}

merge_sdk_dylib() {
  local sdk="$1"; shift
  local archs="$1"; shift
  local merged_dir="$OUT_DIR/$sdk-merged"
  mkdir -p "$merged_dir"

  local inputs=()
  for arch in $archs; do
    inputs+=("$OUT_DIR/$sdk-$arch/${LIB_NAME}.dylib")
  done

  if [ ${#inputs[@]} -eq 1 ]; then
    cp -f "${inputs[0]}" "$merged_dir/${LIB_NAME}.dylib"
  else
    xcrun lipo -create "${inputs[@]}" -output "$merged_dir/${LIB_NAME}.dylib"
  fi

  echo "$merged_dir/${LIB_NAME}.dylib"
}

device_lib="$(merge_sdk_lib iphoneos "$DEVICE_ARCHS")"
sim_lib="$(merge_sdk_lib iphonesimulator "$SIM_ARCHS")"

device_dylib="$(merge_sdk_dylib iphoneos "$DEVICE_ARCHS")"
sim_dylib="$(merge_sdk_dylib iphonesimulator "$SIM_ARCHS")"

XC_OUT="$OUT_DIR/FlintDB.xcframework"

xcodebuild -create-xcframework \
  -library "$device_lib" -headers "$HDR_DIR" \
  -library "$sim_lib" -headers "$HDR_DIR" \
  -output "$XC_OUT"

# Also copy the merged device libs to c/lib with the requested naming.
mkdir -p "$ROOT_DIR/lib"
cp -f "$device_lib" "$ROOT_DIR/lib/${LIB_NAME}.a"
cp -f "$device_dylib" "$ROOT_DIR/lib/${LIB_NAME}.dylib"

echo ""
echo "=== iOS Build Complete ==="
echo "XCFramework: $XC_OUT"
echo "Device static: $ROOT_DIR/lib/${LIB_NAME}.a"
echo "Device dylib : $ROOT_DIR/lib/${LIB_NAME}.dylib"
