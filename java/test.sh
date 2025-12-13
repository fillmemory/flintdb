#!/bin/bash

# ./test.sh --build -c lite.db.tutorial.Examples
# ./test.sh --build -c lite.db.TestcaseLargeRows

JTEST=lite.db.tutorial.Examples
JTEST=lite.db.TestcasePerfAll
#JTEST=lite.db.TestcaseSQL

# if [ -z "$1" ]; then
#   echo "Usage: $0 <test_name>"
#   exit 1
# fi

# check if -c <classname> is passed
for ((i=1; i<=$#; i++)); do
  arg=${!i}
  if [[ $arg == -c ]]; then
    next=$((i+1))
    JTEST=${!next}
    break
  elif [[ $arg == --build ]]; then
    echo "Building project..."
    ./build.sh --test
  fi
done

# check build/classes/java/test
CLASSNAME=$(echo "$JTEST" | sed 's/\./\//g')
echo "Running test: $JTEST"
echo "Class name: $CLASSNAME"
if [ ! -d "build/classes/java/test" ]; then
  if [ -f ./build.sh ]; then
    echo "Running build script to compile tests..."
  else
    echo "Build script not found. Please ensure you are in the correct directory."
    exit 1
  fi
  ./build.sh --test
fi

JLIB_PATH=lib
JVMOPT="-Xmx16g"

# Use Gradle default output directories
CP="build/classes/java/main:build/classes/java/test"
while IFS= read -r -d '' i; do
  CP="${CP}:${i}"
done < <(find "${JLIB_PATH}" -name "*.jar" -print0)

echo "CP: $CP"
java -cp ".:${CP}" $JVMOPT $JTEST "$@"
