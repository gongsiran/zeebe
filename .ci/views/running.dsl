listView('Running') {
  configure { view ->
    // filter
    view / jobFilters << {
      'hudson.views.BuildStatusFilter' {
        includeExcludeTypeString includeMatched
        neverBuilt false
        building true
        inBuildQueue false
      }
    }

    // columns
    view / columns << {
      'hudson.views.StatusColumn' {}
      'hudson.views.WeatherColumn' {}
      'jenkins.plugins.extracolumns.LastBuildConsoleColumn' {}
      'jenkins.plugins.extracolumns.ConfigureProjectColumn' {}
      'hudson.views.JobColumn' {}
      'com.robestone.hudson.compactcolumns.LastSuccessAndFailedColumn' {
        timeAgoTypeString 'DIFF'
      }
      'jenkins.plugins.extracolumns.BuildDurationColumn' {
        buildDurationType 1
      }
      'jenkins.plugins.extracolumns.BuildDurationColumn' {
        buildDurationType 0
      }
    }
  }
}
