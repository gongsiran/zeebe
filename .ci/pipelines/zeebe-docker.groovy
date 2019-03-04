pipeline {

  agent {
    kubernetes {
      cloud 'zeebe-ci'
      label "zeebe-ci-build_${env.JOB_BASE_NAME}-${env.BUILD_ID}"
      defaultContainer 'jnlp'
      yaml '''\
apiVersion: v1
kind: Pod
metadata:
  labels:
    agent: zeebe-ci-build
spec:
  nodeSelector:
    cloud.google.com/gke-nodepool: slaves
  containers:
    - name: maven
      image: maven:3.6.0-jdk-8
      imagePullPolicy: Always
      command: ["cat"]
      tty: true
      resources:
        limits:
          cpu: 500m
          memory: 1Gi
        requests:
          cpu: 500m
          memory: 1Gi
    - name: docker
      image: docker:dind
      args: ["--storage-driver=overlay2"]
      securityContext:
        privileged: true
      tty: true
      resources:
        limits:
          cpu: 1
          memory: 1Gi
        requests:
          cpu: 500m
          memory: 512Mi
'''
    }
  }

  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    timestamps()
    timeout(time: 15, unit: 'MINUTES')
  }

  environment {
      DOCKER_HUB = credentials("camunda-dockerhub")
      VERSION = "${params.VERSION}"
      IS_LATEST = "${params.IS_LATEST}"
  }

  stages {
    stage('Build') {
      steps {
        git url: 'git@github.com:zeebe-io/zeebe',
            branch: "${params.BRANCH}",
            credentialsId: 'camunda-jenkins-github-ssh',
            poll: false

        container('maven') {
            sh ('''\
#!/bin/sh

mvn dependency:get -B \
    -DremoteRepositories="camunda-nexus::::https://app.camunda.com/nexus/content/repositories/public" \
    -DgroupId="io.zeebe" -DartifactId="zeebe-distribution" \
    -Dversion="${VERSION}" -Dpackaging="tar.gz" -Dtransitive=false

mvn dependency:copy -B \
    -Dartifact="io.zeebe:zeebe-distribution:${VERSION}:tar.gz" \
    -DoutputDirectory=${WORKSPACE} \
    -Dmdep.stripVersion=true
''')
        }

        container('docker') {
            sh ('''\
#!/bin/sh -xe

if [ "${VERSION##*-}" = "SNAPSHOT" ]; then
    TAG=SNAPSHOT
else
    TAG=${VERSION}
fi

echo "Building Zeebe Docker image ${TAG}."
docker build --no-cache --build-arg DISTBALL=zeebe-distribution.tar.gz -t camunda/zeebe:${TAG} .

echo "Authenticating with DockerHub and pushing image."
docker login --username ${DOCKER_HUB_USR} --password ${DOCKER_HUB_PSW}

docker push camunda/zeebe:${TAG}

if [ "${IS_LATEST}" = "true" ]; then
    docker tag camunda/zeebe:${TAG} camunda/zeebe:latest
    docker push camunda/zeebe:latest
fi
''')
        }
      }
    }
  }
}
