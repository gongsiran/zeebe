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
          memory: 500Mi
        requests:
          cpu: 500m
          memory: 500Mi
'''
    }
  }

  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    timestamps()
    timeout(time: 15, unit: 'MINUTES')
  }

  stages {
    stage('Build') {
      steps {
        git url: 'git@github.com:zeebe-io/zeebe',
            branch: "${params.BRANCH}",
            credentialsId: 'camunda-jenkins-github-ssh',
            poll: false

        container('maven') {
            sshagent(['docs-zeebe-io-ssh']) {
                sh ("""\
#!/bin/bash -xue

MDBOOK_VERSION=v0.1.5

# go to docs folder
cd docs/

# dowload mdbook
curl -o mdbook -sL https://github.com/zeebe-io/mdBook/releases/download/zeebe-io/mdbook
chmod +x mdbook

# install rsync
apt-get update -qq
apt-get install -qq -y --no-install-recommends rsync

# build docs
./mdbook build

if [ "${params.LIVE}" = "true" ]; then
    FOLDER=docs.zeebe.io
else
    FOLDER=stage.docs.zeebe.io
fi

# upload
rsync -azv --delete-after "book/" jenkins_docs_zeebe_io@vm29.camunda.com:"/var/www/camunda/\${FOLDER}/" -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
""")
          }
        }
      }
    }
  }
}

