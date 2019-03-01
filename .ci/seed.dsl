// vim: set filetype=groovy:

multibranchPipelineJob('zeebe') {

  displayName 'Camunda Zeebe'
  description 'MultiBranchJob for Camunda Zeebe'

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
