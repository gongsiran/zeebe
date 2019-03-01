#!/bin/bash -xeu

export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -XX:MaxRAMFraction=$((LIMITS_CPU))"

mvn -B -T2 -s settings.xml -DskipTests -Dskip-zbctl=false \
  clean com.mycila:license-maven-plugin:check com.coveo:fmt-maven-plugin:check install

mvn -B -T$LIMITS_CPU -s settings.xml verify -P skip-unstable-ci,retry-tests,parallel-tests
