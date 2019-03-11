package io.zeebe.distributedlog.impl;

import io.zeebe.logstreams.log.LogStream;
import java.util.HashMap;
import java.util.Map;

public class LogstreamConfig {

  private static final Map<String, LogStream> LOG_STREAMS = new HashMap<>();

  public static void putLogStream(String nodeId, String partitionId, LogStream logStream){
    LOG_STREAMS.put(logPropertyName(nodeId,partitionId), logStream);
  }

  public static LogStream getLogStream(String nodeId, String partitionId) {
    return LOG_STREAMS.get(logPropertyName(nodeId, partitionId));
  }

  private static String logPropertyName(String nodeId, String partitionId){
    return String.format("log-%s-%s", nodeId, partitionId);
  }

}
