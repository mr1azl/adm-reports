#!/bin/bash

JVM_OPTS="-Xms2g -Xmx2g "

GC_OPTS="\
-XX:+UseG1GC \
-XX:+ParallelRefProcEnabled \
-XX:G1HeapRegionSize=8m \
-XX:MaxGCPauseMillis=100 \
"

## TODO add runner script

mvn clean package

java $JVM_OPTS $GC_OPTS -jar target/analytics-server-0.1.jar
