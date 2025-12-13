#!/bin/bash
#
# ./bin/pipeline.sh <pipeline-xml-file> ...
# ./bin/pipeline.sh temp/pipeline-applogs.xml

SCRIPT="${BASH_SOURCE[0]}"
SCRIPTDIR=$(dirname $SCRIPT)
SCRIPTDIR=$(realpath ${SCRIPTDIR})
SCRIPTDIR=$(dirname ${SCRIPTDIR})

JLIB_PATHS=(${SCRIPTDIR}/lib ${SCRIPTDIR}/build)
JMAIN=lite.db.pipeline.DataPipeline
JVMOPT="-Dfile.encoding=UTF-8 -Xmx6g -Dorg.slf4j.simpleLogger.defaultLogLevel=error"

CP=.
for i in ${JLIB_PATHS[@]}
do
  JLIB_PATH=$i
  if [ -d ${JLIB_PATH} ]; then
    for j in `find ${JLIB_PATH} -type f -name "*.jar"`
    do
      CP=${CP}:${j}
    done
  fi
done

java -cp $CP $JVMOPT $JMAIN "$@"
