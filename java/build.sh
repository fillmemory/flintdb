#!/usr/bin/env bash

set -euo pipefail

SCRIPT_NAME="$(basename "$0")"

usage() {
  cat <<USAGE
Usage: ./$SCRIPT_NAME [options]

Options:
  --test        Build with tests (default build skips tests)
  --javadoc     Build javadoc and javadoc JAR (skips tests)
  --pack        Package artifacts into build/pack and create build/pack/flintdb.tar.gz
  --help, -h    Show this help

Behavior:
  - Default: gradle clean build jar javadocjar -x test
  - --test:  gradle clean build jar (tests enabled)
  - --javadoc: gradle clean build jar javadoc javadocjar -x test
  - Gradle wrapper (./gradlew) is used if present, otherwise system 'gradle'.
USAGE
}

log() { printf "[build.sh] %s\n" "$*"; }
die() { printf "[build.sh] ERROR: %s\n" "$*" >&2; exit 1; }

# Prefer gradle wrapper if available
GRADLE_CMD="gradle"
if [[ -x "./gradlew" ]]; then
  GRADLE_CMD="./gradlew"
fi

# Parse flags
RUN_TESTS=false
WITH_JAVADOC=false
DO_PACK=false

for arg in "$@"; do
  case "$arg" in
    --help|-h) usage; exit 0 ;;
    --test) RUN_TESTS=true ;;
    --javadoc) WITH_JAVADOC=true ;;
    --pack) DO_PACK=true ;;
    *) die "Unknown option: $arg (use --help)" ;;
  esac
done

# Sanity check project root
[[ -f "build.gradle" ]] || die "build.gradle not found in current directory: $(pwd)"

run_gradle() {
  log "Running: $GRADLE_CMD $* --no-daemon"
  "$GRADLE_CMD" "$@" --no-daemon
}

build_default() {
  run_gradle clean build jar -x test
}

build_with_tests() {
  run_gradle clean build jar
}

build_with_javadoc() {
  run_gradle clean build jar javadoc javadocjar -x test
}

pack_artifacts() {
  local PACK_DIR="./build/pack"
  log "Packaging artifacts into $PACK_DIR"
  rm -rf "$PACK_DIR"
  mkdir -p "$PACK_DIR/lib"
  mkdir -p "$PACK_DIR/tutorial/src"

  # Include scripts and jars
  if [[ -d ./bin ]]; then cp -R ./bin "$PACK_DIR/"; fi
  if compgen -G "./lib/*.sh" > /dev/null; then cp -R ./lib/*.sh "$PACK_DIR/lib/"; fi
  if compgen -G "./build/*.jar" > /dev/null; then cp -R ./build/*.jar "$PACK_DIR/lib/"; fi

  find ./tutorial -type f -name '*.java' -exec cp {} "$PACK_DIR/tutorial/src/" \;
  find ./tutorial -type f -name 'tutorial.sh'       -exec cp {} "$PACK_DIR" \;
  find ./tutorial -type f -name 'tutorial.bat'      -exec cp {} "$PACK_DIR" \;

  cp ../LICENSE "$PACK_DIR/"
  cp ../README.md "$PACK_DIR/"

  find "$PACK_DIR" -type f -name '*.sh' -exec chmod a+x {} +
  find "$PACK_DIR" -type f -name ".DS_Store" -exec rm -f {} \;

  # Remove main and test scripts
  rm -rf "$PACK_DIR/bin/main" "$PACK_DIR/bin/test" 

  # Create tarball
  ( cd "$PACK_DIR" && tar -czf ../flintdb.tar.gz ./* )
  log "Created $PACK_DIR/flintdb.tar.gz"
}

list_jar_sizes() {
  if [[ -d ./build ]]; then
    find ./build -type f -name "*.jar" -exec du -h {} \;
  fi
}

# Build flow
if [[ "$WITH_JAVADOC" == true && "$RUN_TESTS" == true ]]; then
  log "Note: --javadoc implies skipping tests; ignoring --test"
fi

if [[ "$WITH_JAVADOC" == true ]]; then
  build_with_javadoc
elif [[ "$RUN_TESTS" == true ]]; then
  build_with_tests
else
  build_default
fi

if [[ "$DO_PACK" == true ]]; then
  if [[ "$WITH_JAVADOC" == false && "$RUN_TESTS" == false ]]; then
    pack_artifacts
  else
    log "Ignoring --pack when used with --test or --javadoc (preserving original behavior)"
  fi
fi

list_jar_sizes
