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
        git url: 'git@github.com:zeebe-io/zeebe.io',
            branch: "${params.BRANCH}",
            credentialsId: 'camunda-jenkins-github-ssh',
            poll: false

        container('maven') {
            sshagent(['docs-zeebe-io-ssh']) {
                sh ("""\
#!/bin/bash -xue

HUGO_VERSION=0.30.2

# dowload hugo
curl -L https://github.com/gohugoio/hugo/releases/download/v0.30.2/hugo_0.30.2_Linux-64bit.tar.gz -o hugo_0.30.2_Linux-64bit.tar.gz
tar xvzf hugo_0.30.2_Linux-64bit.tar.gz
chmod +x ./hugo

if [ "${params.LIVE}" = "true" ]; then
    BASE=https://zeebe.io/
    FOLDER=www.zeebe.io
else
    BASE=https://stage.zeebe.io/
    FOLDER=stage.zeebe.io
fi

# build docs
./hugo -b ${BASE}

# upload
rsync -azv --delete-after "public/" jenkins_docs_zeebe_io@vm29.camunda.com:"/var/www/camunda/\${FOLDER}/" -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
""")
          }
        }
      }
    }
  }
}

