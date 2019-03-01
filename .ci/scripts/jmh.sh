#!/bin/bash -eux

export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -XX:MaxRAMFraction=2"

mvn -B -s settings.xml integration-test -DskipTests -P jmh

cat **/*/jmh-result.json | jq -s add > target/jmh-result.json
