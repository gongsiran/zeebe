// vim: set filetype=groovy:

multibranchPipelineJob('zeebe') {

  displayName 'zeebe-io/zeebe'
  description 'MultiBranchJob for Zeebe'

  // https://ci.zeebe.camunda.cloud/plugin/job-dsl/api-viewer/index.html#path/javaposse.jobdsl.dsl.jobs.MultibranchWorkflowJob.branchSources-branchSource-source
  branchSources {
    github {
      repoOwner 'zeebe-io'
      repository 'zeebe'

      includes 'cloud-ci'
      excludes '.*.tmp'

      scanCredentialsId 'camunda-jenkins-github'
      checkoutCredentialsId 'camunda-jenkins-github-ssh'

      buildForkPRHead true
      buildForkPRMerge false

      buildOriginPRHead true
      buildOriginPRMerge false
    }
  }

  orphanedItemStrategy {
    discardOldItems {
      numToKeep 20
    }
    defaultOrphanedItemStrategy {
      pruneDeadBranches true
      daysToKeepStr '1'
      numToKeepStr '20'
    }
  }

  triggers {
    periodic(1440) // Minutes - Re-index once a day, if not triggered before
  }
}

pipelineJob('zeebe-RELEASE-pipeline')
{
    displayName 'Zeebe Release Pipeline'
    definition {
        cps {
            script(readFileFromWorkspace('.ci/pipelines/zeebe-release.groovy'))
            sandbox()
        }
    }

    parameters
    {
        stringParam('RELEASE_VERSION', '0.X.0', 'Zeebe version to release.')
        stringParam('DEVELOPMENT_VERSION', '0.Y.0-SNAPSHOT', 'Next Zeebe development version.')
        booleanParam('IS_LATEST', true, 'Should the docker image of this release be tagged as latest?')
        booleanParam('PUSH_CHANGES', true, 'If TRUE, push the changes to remote repositories. If FALSE, do not push changes to remote repositories. Must be used in conjunction with USE_LOCAL_CHECKOUT = TRUE to test the release!')
        booleanParam('USE_LOCAL_CHECKOUT', false, 'If TRUE, uses the local git repository to checkout the release tag to build.  If FALSE, checks out the release tag from the remote repositoriy. Must be used in conjunction with PUSH_CHANGES = FALSE to test the release!')
        booleanParam('SKIP_DEPLOY_TO_MAVEN_CENTRAL', false, 'If TRUE, skip the deployment to maven central. Should be used when testing the release.')
        booleanParam('SKIP_DEPLOY_TO_CAMUNDA_NEXUS', false, 'If TRUE, skip the deployment to Camunda nexus. Should be used when testing the release.')
    }
}

pipelineJob('zeebe-docs')
{
    displayName 'Zeebe Documentation'
    definition {
        cps {
            script(readFileFromWorkspace('.ci/pipelines/zeebe-docs.groovy'))
            sandbox()
        }
    }

    parameters
    {
        stringParam('BRANCH', 'develop', 'zeebe-io/zeebe branch to build and push')
        booleanParam('LIVE', false, 'Should the docs be pushed to https://docs.zeebe.io')
    }
}

pipelineJob('zeebe-docker')
{
    displayName 'Zeebe Docker Image'
    definition {
        cps {
            script(readFileFromWorkspace('.ci/pipelines/zeebe-docker.groovy'))
            sandbox()
        }
    }

    parameters
    {
        stringParam('BRANCH', 'develop', 'zeebe-io/zeebe branch to build')
        stringParam('VERSION', '', 'Zeebe version to build the image for')
        booleanParam('IS_LATEST', false, 'Should the docker image be tagged as latest')
    }
}
