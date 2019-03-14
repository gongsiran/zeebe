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

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.ServiceContainer;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);

  private LogStream logStream;
  private LogStorage logStorage;
  private long lastPosition;

  private String logName;

   private final int partition;

  private final DistributedLogstreamServiceConfig config;
  private ServiceContainer serviceContainer;

  public DefaultDistributedLogstreamService(DistributedLogstreamServiceConfig config) {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
     logName = config.getLogName();
     partition = config.getPartition();
    this.config = config;
    lastPosition = -1;
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);
    logName = getRaftPartitionName(executor);
    LOG.info(
        "Configuring DistLog {} on node {} with logName {}",
        getServiceName(),
        getLocalMemberId().id(),
        logName);
    try {
      createLogStream(String.format("partition-%d",partition));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private String getRaftPartitionName(ServiceExecutor executor) {
    String name = getServiceName();
    try {
      final Field context = DefaultServiceExecutor.class.getDeclaredField("context");
      context.setAccessible(true);
      final RaftServiceContext raftServiceContext = (RaftServiceContext) context.get(executor);
      final Field raft = RaftServiceContext.class.getDeclaredField("raft");
      raft.setAccessible(true);
      RaftContext raftContext = (RaftContext) raft.get(raftServiceContext);
      name = raftContext.getName();
      raft.setAccessible(false);
      context.setAccessible(false);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return name;
  }

  private void createLogStream(String logServiceName) {
    String localmemberId = getLocalMemberId().id();
    serviceContainer = LogstreamConfig.getServiceContainer(localmemberId);
    String partitionDirectory = LogstreamConfig.getLogDirectory(localmemberId) + "/" + logServiceName;

    final File logDirectory = new File(partitionDirectory, logServiceName);
    logDirectory.mkdir();

    final File snapshotDirectory = new File(partitionDirectory, logServiceName + "-snapshot");
    snapshotDirectory.mkdir();

    logStream =
        LogStreams.createFsLogStream(partition) //FIXME: Get partitionId from raftName. OR Do we need partitionId to create logstreams??
            .logDirectory(logDirectory.getAbsolutePath())
            .logSegmentSize(config.getLogSegmentSize())
            .logName(logServiceName)
            .serviceContainer(serviceContainer)
            .snapshotStorage(
                LogStreams.createFsSnapshotStore(snapshotDirectory.getAbsolutePath()).build())
            .build()
            .join();
    this.logStorage = this.logStream.getLogStorage();

    // Attempt to handle replayed appends
    final BufferedLogStreamReader reader = new BufferedLogStreamReader(logStream);
    reader.seekToLastEvent();
    lastPosition = reader.getPosition(); // This is not the last commit position

    LOG.info("Logstreams created");
  }

  /* private void initialize() {
    try {
      this.logStream = LogstreamConfig.getLogStream(getLocalMemberId().id(), logName);
      this.logStorage = this.logStream.getLogStorage();
      final BufferedLogStreamReader reader = new BufferedLogStreamReader(logStream);
      reader.seekToLastEvent();
      lastPosition = reader.getPosition();
    } catch (NullPointerException e) {
      // FIXME
    }
  }*/

  @Override
  public void append(long commitPosition, byte[] blockBuffer) {
    try {
      if (commitPosition <= lastPosition) {
        return;
      }
      final ByteBuffer buffer = ByteBuffer.wrap(blockBuffer);
      logStorage.append(buffer);
      // TODO: (https://github.com/zeebe-io/zeebe/issues/2058)
      logStream.signalOnAppendCondition();
      // Commit position may be not required anymore. https://github.com/zeebe-io/zeebe/issues/2058.
      // Following is required to trigger the commit listeners.
      logStream.setCommitPosition(commitPosition);
      LOG.info("Appending to log {} position {} ", logName, commitPosition);
      lastPosition = commitPosition;
    } catch (NullPointerException e) {
      // TODO: send failure response
    }
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    // TODO
  }

  @Override
  public void restore(BackupInput backupInput) {
    // TODO
  }

  @Override
  public void close() {
    super.close();
    LOG.info("Closing {}", getServiceName());
  }
}
