/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.impl;

import io.zeebe.logstreams.log.LogStream;
import java.util.HashMap;
import java.util.Map;

public class LogstreamConfig {

  private static final Map<String, LogStream> LOG_STREAMS = new HashMap<>();

  public static void putLogStream(String nodeId, String partitionId, LogStream logStream) {
    LOG_STREAMS.put(logPropertyName(nodeId, partitionId), logStream);
  }

  public static void removeLogStream(String nodeId, String partitionId) {
    LOG_STREAMS.remove(logPropertyName(nodeId, partitionId));
  }

  public static LogStream getLogStream(String nodeId, String partitionId) {
    return LOG_STREAMS.get(logPropertyName(nodeId, partitionId));
  }

  private static String logPropertyName(String nodeId, String partitionId) {
    return String.format("log-%s-%s", nodeId, partitionId);
  }
}
