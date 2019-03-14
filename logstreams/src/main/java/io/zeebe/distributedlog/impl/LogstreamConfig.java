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
import io.zeebe.servicecontainer.ServiceContainer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LogstreamConfig {

  private static final Map<String, LogStream> LOG_STREAMS = new ConcurrentHashMap<>();
  private static final Map<String, String> LOG_DIRS = new ConcurrentHashMap<>();
  private static final Map<String, ServiceContainer> SERVICE_CONTAINERS = new ConcurrentHashMap<>();

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

  public static void putLogDirectory(String nodeId, String dirPath) {
    LOG_DIRS.put(nodeId, dirPath);
  }

  public static String getLogDirectory(String nodeId) {
    return LOG_DIRS.get(nodeId);
  }

  public static void putServiceContainer(String nodeId, ServiceContainer serviceContainer) {
    SERVICE_CONTAINERS.put(nodeId, serviceContainer);
  }

  public static ServiceContainer getServiceContainer(String nodeId) {
    return SERVICE_CONTAINERS.get(nodeId);
  }
}
