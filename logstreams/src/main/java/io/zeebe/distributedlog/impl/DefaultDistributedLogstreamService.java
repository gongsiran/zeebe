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
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
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

  private final String logName;

  public DefaultDistributedLogstreamService(DistributedLogstreamServiceConfig config) {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
    logName = config.getLogName();
    lastPosition = -1;
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);
    LOG.info("Configuring DistLog {} on node {} ", logName, getLocalMemberId().id());
    try {
      // FIXME. when a node restarts this primitive is configured before the logstreams are
      // initialized.
      initialize();
    } catch (Exception e) {
      //     e.printStackTrace();

    }
  }

  private void initialize() {
    try {
      this.logStream = LogstreamConfig.getLogStream(getLocalMemberId().id(), logName);
      this.logStorage = this.logStream.getLogStorage();
      final BufferedLogStreamReader reader = new BufferedLogStreamReader(logStream);
      reader.seekToLastEvent();
      lastPosition = reader.getPosition();
    } catch (NullPointerException e) {
      // FIXME
    }
  }

  @Override
  public void append(long commitPosition, byte[] blockBuffer) {
    if (logStorage == null) {
      //FIXME:
   //   initialize();
    }
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
