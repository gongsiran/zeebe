package io.zeebe.distributedlog.impl;

import io.zeebe.logstreams.log.LogStream;
import java.util.HashMap;
import java.util.Map;

public class LogstreamConfig {

  private static final Map<String, LogStream> logStreams = new HashMap<>();

  public static void setLogStream(String nodeId, String partitionId, LogStream logStream){
    logStreams.put(logPropertyName(nodeId,partitionId), logStream);
  }

  public static LogStream getLogStream(String nodeId, String partitionId) {
    return logStreams.get(logPropertyName(nodeId, partitionId));
  }

  private static String logPropertyName(String nodeId, String partitionId){
    return String.format("log-%s-%s", nodeId, partitionId);
  }

}
