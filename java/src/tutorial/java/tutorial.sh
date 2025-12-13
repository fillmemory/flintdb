#!/bin/bash

# Simple script to compile and generate the tutorial files.
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"
echo "PWD:" $(pwd)

LIB_DIRS=(./lib)

CP="./tutorial/classes"
for dir in "${LIB_DIRS[@]}"; do
  if [ -d "$dir" ]; then
    for i in $(find "$dir" -name "*.jar"); do
      CP="$CP:$i"
    done
  fi
done

echo "CP :" $CP

javac -cp "$CP" tutorial/src/Tutorial.java -d tutorial/classes
# java  -cp "$CP" Tutorial --help
java  -cp "$CP" Tutorial --customers=10000 --avg-orders=10 --avg-items=30

