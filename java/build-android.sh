#!/bin/bash
set -e

# Configuration
JAVA_VERSION=11
BUILD_DIR="build/java-android"
SRC_DIRS="src/android/java src/main/java"
EXCLUDE_FILES=("JdbcConfig.java" "JdbcTable.java" "IoBuffer.java")
VERSION="0.1.$(date +%y%m%d)"
JAR_NAME="flintdb-java11-${VERSION}.jar"

echo "Building FlintDB Android JAR with Java ${JAVA_VERSION}"
echo "Output: ${BUILD_DIR}/${JAR_NAME}"

# Clean and create build directories
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}/classes"
mkdir -p "${BUILD_DIR}/sources"

# Collect source files
echo "Collecting source files..."

# Add all files from src/android/java
find src/android/java -name "*.java" > "${BUILD_DIR}/sources/files.txt"

# Add files from src/main/java excluding duplicates
for exclude in "${EXCLUDE_FILES[@]}"; do
    find src/main/java -name "${exclude}" >> "${BUILD_DIR}/sources/exclude.txt"
done

find src/main/java -name "*.java" | while read file; do
    excluded=false
    for exclude in "${EXCLUDE_FILES[@]}"; do
        if [[ "$file" == *"/${exclude}" ]]; then
            excluded=true
            break
        fi
    done
    if [ "$excluded" = false ]; then
        echo "$file" >> "${BUILD_DIR}/sources/files.txt"
    fi
done

# Count files
FILE_COUNT=$(wc -l < "${BUILD_DIR}/sources/files.txt")
echo "Compiling ${FILE_COUNT} Java files..."

# Compile
javac --release ${JAVA_VERSION} \
    -d "${BUILD_DIR}/classes" \
    -encoding UTF-8 \
    @"${BUILD_DIR}/sources/files.txt"

# Create JAR
echo "Creating JAR: ${JAR_NAME}"
cd "${BUILD_DIR}/classes"
jar cfe "../${JAR_NAME}" flint.db.CLI \
    -C . .

cd ../../..
echo "Build successful: ${BUILD_DIR}/${JAR_NAME}"
ls -lh "${BUILD_DIR}"/*.jar
