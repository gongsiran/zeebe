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

import com.google.common.collect.Sets;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.zeebe.distributedlog.CommitLogEvent;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);

  protected Set<SessionId> listeners = Sets.newLinkedHashSet();
  private LogStream logStream;
  private LogStorage logStorage;

  public DefaultDistributedLogstreamService(DistributedLogstreamServiceConfig config) {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);
    String localMemberId = getLocalMemberId().id();
    String localPartitionName;
    try {
      final Field context = DefaultServiceExecutor.class.getDeclaredField("context");
      context.setAccessible(true);
      final RaftServiceContext raftServiceContext = (RaftServiceContext) context.get(executor);
      final Field raft = RaftServiceContext.class.getDeclaredField("raft");
      raft.setAccessible(true);
      RaftContext raftContext = (RaftContext) raft.get(raftServiceContext);
      localPartitionName = raftContext.getName();
      LOG.error("configure {}", localPartitionName);
      raft.setAccessible(false);
      context.setAccessible(false);

      this.logStream = LogstreamConfig.getLogStream(localMemberId, localPartitionName);
      if (logStream != null) {
        LOG.info(
            "Logstream name {}, partitionid {}, raft name {}",
            logStream.getLogName(),
            logStream.getPartitionId(),
            localPartitionName);

        logStorage = this.logStream.getLogStorage();
      }
      else{
        LOG.error("logstream is null");
      }
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void append(long commitPosition, byte[] blockBuffer) {
    // Publish the committed log entries to the listeners who will write to the logStorage.
    final ByteBuffer buffer = ByteBuffer.wrap(blockBuffer);
    logStorage.append(buffer);
    // TODO: (https://github.com/zeebe-io/zeebe/issues/2058)
    logStream.signalOnAppendCondition();
    // Commit position may be not required anymore. https://github.com/zeebe-io/zeebe/issues/2058.
    // Following is required to trigger the commit listeners.
    logStream.setCommitPosition(commitPosition);
    //publish(commitPosition, blockBuffer);
  }

  @Override
  public void listen() {
    listeners.add(getCurrentSession().sessionId());
  }

  @Override
  public void unlisten() {
    listeners.remove(getCurrentSession().sessionId());
  }

  private void publish(long commitPosition, byte[] appendBytes) {
    final CommitLogEvent commitLogEvent = new CommitLogEvent(commitPosition, appendBytes);
    listeners.forEach(
        listener -> getSession(listener).accept(client -> client.change(commitLogEvent)));
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
