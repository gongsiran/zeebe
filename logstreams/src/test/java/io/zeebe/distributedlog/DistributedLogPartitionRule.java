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
package io.zeebe.distributedlog;

import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.distributedLogPartitionServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageAppenderRootService;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStorageAppenderServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamRootServiceName;
import static io.zeebe.logstreams.impl.service.LogStreamServiceNames.logStreamServiceName;
import static io.zeebe.util.buffer.BufferUtil.bufferAsString;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.distributedlog.impl.DistributedLogstreamPartition;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.test.util.TestUtil;
import io.zeebe.util.sched.future.ActorFuture;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogPartitionRule {

  private final ServiceContainer serviceContainer;
  private final int partition;
  private final int nodeId;
  private final String dir;
  private LogStream logStream;
  private BufferedLogStreamReader reader;
  private LogStreamWriterImpl writer = new LogStreamWriterImpl();

  private final RecordMetadata metadata = new RecordMetadata();
  public static final Logger LOG = LoggerFactory.getLogger("io.zeebe.distributedlog.test");
  private final String logName;
  // private final String dir;

  public DistributedLogPartitionRule(
      ServiceContainer serviceContainer, int nodeId, int partition, Path rootDirectory)
      throws IOException {
    this.serviceContainer = serviceContainer;
    this.nodeId = nodeId;
    this.partition = partition;
    this.logName = String.format("raft-atomix-partition-%d", this.partition);
    final File logDir = new File(rootDirectory.toString(), String.format("log-%d", partition));
    if (!logDir.exists()) {
      Files.createDirectory(logDir.toPath());
    }
    dir = logDir.toPath().toString();
  }

  public void start() throws IOException {
    createLogStream();
  }

  public void close() {
    if (serviceContainer.hasService(logStorageAppenderRootService(logName))) {
      logStream.closeAppender().join(); // If opened
    }
    if (serviceContainer.hasService(distributedLogPartitionServiceName(logName))) {
      serviceContainer.removeService(distributedLogPartitionServiceName(logName));
    }
    if (serviceContainer.hasService(logStreamRootServiceName(logName))) {
      logStream.close();
    }
  }

  private void createLogStream() throws IOException {
    final String memberId = String.valueOf(nodeId);
    LogstreamConfig.putLogDirectory(memberId, dir);
    LogstreamConfig.putServiceContainer(memberId, serviceContainer);

    final DistributedLogstreamPartition log = new DistributedLogstreamPartition(partition);
    final ActorFuture<DistributedLogstreamPartition> installFuture =
        serviceContainer
            .createService(distributedLogPartitionServiceName(logName), log)
            .dependency(DistributedLogRule.ATOMIX_SERVICE_NAME, log.getAtomixInjector())
            // .dependency(logStreamServiceName(logName), log.getLogStreamInjector())
            .install();

    /*final ActorFuture<LogStream> logStreamFuture =
            LogStreams.createFsLogStream(partition)
                .logName(logName)
                .deleteOnClose(false)
                .logDirectory(dir)
                .serviceContainer(serviceContainer)
                .build();
        LOG.info("Build logstreams node {} log {}", nodeId, dir);
        logStream = logStreamFuture.join();
        LOG.info("Build logstream completed");
    */
    installFuture.join();

    final ServiceName<Void> testService =
        ServiceName.newServiceName(String.format("test-%s", logName), Void.class);
    final Injector<LogStream> logStreamInjector = new Injector<>();

    serviceContainer
        .createService(testService, () -> null)
        .dependency(logStreamServiceName("raft-atomix-partition-" + partition), logStreamInjector)
        .dependency(distributedLogPartitionServiceName(logName))
        .install()
        .join();

    logStream = logStreamInjector.getValue();
    assertThat(logStream).isNotNull();
    reader = new BufferedLogStreamReader(logStream);
  }

  public void becomeLeader() {
    logStream.openAppender();

    TestUtil.waitUntil(
        () -> serviceContainer.hasService(logStorageAppenderServiceName(logName)), 250);
  }

  public boolean eventAppended(String message, long writePosition) {
    reader.seek(writePosition);
    if (reader.hasNext()) {
      final LoggedEvent event = reader.next();
      final String messageRead =
          bufferAsString(event.getValueBuffer(), event.getValueOffset(), event.getValueLength());
      final long eventPosition = event.getPosition();
      return (message.equals(messageRead) && eventPosition == writePosition);
    }
    return false;
  }

  public long writeEvent(final String message) {
    writer.wrap(logStream);

    final AtomicLong writePosition = new AtomicLong();
    final DirectBuffer value = wrapString(message);

    TestUtil.doRepeatedly(
            () -> writer.positionAsKey().metadataWriter(metadata.reset()).value(value).tryWrite())
        .until(
            position -> {
              if (position != null && position >= 0) {
                writePosition.set(position);
                return true;
              } else {
                return false;
              }
            },
            "Failed to write event with message {}",
            message);
    return writePosition.get();
  }

  public int getCommittedEventsCount() {
    int numEvents = 0;
    reader.seekToFirstEvent();
    while (reader.hasNext()) {
      reader.next();
      numEvents++;
    }
    return numEvents;
  }
}
