#!/bin/bash -eux

RESULT_FILE=target/jmh-result.json

# Check inside script instead of pipeline to workaround the UI bug
# https://issues.jenkins-ci.org/browse/JENKINS-48879
if [[ ! "${GIT_BRANCH}" =~ develop|master ]]; then
    echo "Skipping JMH on branch ${GIT_BRANCH}"
    mkdir -p $(dirname ${RESULT_FILE})
    touch ${RESULT_FILE}
    exit 0
fi

export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -XX:MaxRAMFraction=2"

mvn -B -s settings.xml integration-test -DskipTests -P jmh

cat **/*/jmh-result.json | jq -s add > ${RESULT_FILE}
