def mavenRelease = """\
mvn release:prepare release:perform -Dgpg.passphrase="\${GPG_PASSPHRASE}" -B \
    -Dresume=false \
    -Dskip-zbctl=false \
    -Dtag=${params.RELEASE_VERSION} \
    -DreleaseVersion=${params.RELEASE_VERSION} \
    -DdevelopmentVersion=${params.DEVELOPMENT_VERSION} \
    -DpushChanges=${params.PUSH_CHANGES} \
    -DremoteTagging=${params.PUSH_CHANGES} \
    -DlocalCheckout=${params.USE_LOCAL_CHECKOUT} \
    -Darguments='--settings=\${NEXUS_SETTINGS} -DskipTests=true -Dskip-zbctl=false -Dgpg.passphrase="\${GPG_PASSPHRASE}" -Dskip.central.release=${params.SKIP_DEPLOY_TO_MAVEN_CENTRAL} -Dskip.camunda.release=${params.SKIP_DEPLOY_TO_CAMUNDA_NEXUS}'
"""

node('ubuntu-large')
{
    try
    {
        timeout(time: 1, unit: 'HOURS')
        {
            timestamps
            {
                sshagent(['camunda-jenkins-github-ssh'])
                {
                    configFileProvider([
                        configFile(fileId: 'camunda-maven-settings', variable: 'NEXUS_SETTINGS')])
                    {
                        withCredentials([
                            string(credentialsId: 'password_maven_central_gpg_signing_key', variable: 'GPG_PASSPHRASE'),
                            file(credentialsId: 'maven_central_gpg_signing_key', variable: 'MVN_CENTRAL_GPG_KEY_SEC'),
                            file(credentialsId: 'maven_central_gpg_signing_key_pub', variable: 'MVN_CENTRAL_GPG_KEY_PUB'),
                            string(credentialsId: 'github-camunda-jenkins-token', variable: 'GITHUB_TOKEN')])
                        {
                            stage('Prepare')
                            {
                                sh '.ci/scripts/git-config.sh'
                                sh '.ci/scripts/gpg-keys.sh'
                            }

                            stage('zeebe')
                            {
                                git branch: "release-${params.RELEASE_VERSION}", credentialsId: 'camunda-jenkins-github-ssh', url: "git@github.com:zeebe-io/zeebe.git"
                                withMaven(jdk: 'jdk-8-latest', maven: 'maven-3.5-latest', mavenSettingsConfig: 'camunda-maven-settings')
                                {
                                    sh '.ci/scripts/build-zbctl.sh'
                                    sh '.ci/scripts/changelog.sh'
                                    sh mavenRelease
                                }
                            }

                            if (params.PUSH_CHANGES)
                            {
                                stage('GitHub Release')
                                {
                                    sh '.ci/scripts/github-release.sh'
                                }

                                stage('Build Docker Image')
                                {
                                    build job: 'zeebe-docker', parameters: [
                                        string(name: 'BRANCH', value: env.BRANCH_NAME),
                                        string(name: 'VERSION', value: params.RELEASE_VERSION),
                                        booleanParam(name: 'IS_LATEST', value: params.IS_LATEST)
                                    ]
                                }

                                stage('Publish Docs')
                                {
                                    build job: 'zeebe-docs', parameters: [
                                        string(name: 'BRANCH', value: env.BRANCH_NAME)
                                        booleanParam(name: 'LIVE', value: params.IS_LATEST)
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    catch(e)
    {
        emailext recipientProviders: [[$class: 'RequesterRecipientProvider']], subject: "Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' failed", body: "Check console output at ${env.BUILD_URL}"
        throw e
    }
}
