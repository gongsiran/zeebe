#!/bin/sh

TAG=${RELEASE_VERSION:-SNAPSHOT}

docker build --build-arg DISTBALL=dist/target/zeebe-distribution-*.tar.gz -t camunda/zeebe:${TAG} .

docker login --username ${DOCKER_HUB_USR} --password ${DOCKER_HUB_PSW}

docker push camunda/zeebe:${TAG}
