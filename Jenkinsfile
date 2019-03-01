// vim: set filetype=groovy:


def buildName = "${env.JOB_BASE_NAME.replaceAll("%2F", "-").replaceAll("\\.", "-").take(20)}-${env.BUILD_ID}"

pipeline {
    agent none

    environment {
      NEXUS = credentials("camunda-nexus")
    }

    options {
        buildDiscarder(logRotator(daysToKeepStr: '14', numToKeepStr: '10'))
        timestamps()
        timeout(time: 45, unit: 'MINUTES')
    }

    stages {
        stage('Zeebe CI') {
            parallel {
                stage('1 - Java') {
                    agent {
                      kubernetes {
                        cloud 'zeebe-ci'
                        label "zeebe-ci-build_java_${buildName}"
                        defaultContainer 'jnlp'
                        yamlFile '.ci/podSpecs/java.yml'
                      }
                    }

                    stages {
                        stage('Build') {
                            steps {
                                container('maven') {
                                    sh '.ci/scripts/java.sh'
                                }
                            }

                            post {
                                always {
                                    junit testResults: "**/*/TEST-*.xml", keepLongStdio: true
                                }
                            }
                        }

                        stage('Deploy') {
                            steps {
                                container('maven') {
                                    sh 'pwd'
                                    sh 'ls'
                                    sh 'echo mvn deploy'
                                }
                            }
                        }

                        stage('Docker') {
                            steps {
                                container('docker') {
                                    sh 'pwd'
                                    sh 'ls'
                                    sh 'echo docker build'
                                }
                            }
                        }
                    }
                }

                stage('2 - Go') {
                    agent {
                      kubernetes {
                        cloud 'zeebe-ci'
                        label "zeebe-ci-build_go_${buildName}"
                        defaultContainer 'jnlp'
                        yamlFile '.ci/podSpecs/go.yml'
                      }
                    }

                    steps {
                        container('golang') {
                            sh '.ci/scripts/go.sh'
                        }
                    }

                    post {
                        always {
                            junit testResults: "**/*/TEST-*.xml", keepLongStdio: true
                        }
                    }
                }

                stage('3 - JMH') {
                    when { anyOf { branch 'master'; branch 'develop' } }

                    agent {
                      kubernetes {
                        cloud 'zeebe-ci'
                        label "zeebe-ci-build_jmh_${buildName}"
                        defaultContainer 'jnlp'
                        yamlFile '.ci/podSpecs/jmh.yml'
                      }
                    }

                    steps {
                        container('maven') {
                            sh '.ci/scripts/jmh.sh'
                        }
                    }

                    post {
                        success {
                            jmhReport 'target/jmh-result.json'
                        }
                    }
                }
            }
        }
    }
}

void sendBuildStatusNotificationToDevelopers(String buildStatus = 'SUCCESS') {
    def buildResult = buildStatus ?: 'SUCCESS'
    def subject = "${buildResult}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'"
    def details = "${buildResult}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' see console output at ${env.BUILD_URL}'"

    emailext (
        subject: subject,
        body: details,
        recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
    )
}
