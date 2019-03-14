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
package io.zeebe.logstreams.impl;

import io.zeebe.dispatcher.BlockPeek;
import io.zeebe.dispatcher.Subscription;
import io.zeebe.distributedlog.impl.DistributedLogstreamPartition;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.channel.ActorConditions;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

/** Consume the write buffer and append the blocks to the distributedlog. */
public class LogStorageAppender extends Actor {
  public static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

  private final AtomicBoolean isFailed = new AtomicBoolean(false);

  private final BlockPeek blockPeek = new BlockPeek();

  private final String name;
  private final LogStorage logStorage;
  private final Subscription writeBufferSubscription;
  private final ActorConditions logStorageAppendConditions;

  private Runnable peekedBlockHandler = this::appendBlock;
  private int maxAppendBlockSize;

  private final DistributedLogstreamPartition distributedLog;

  public LogStorageAppender(
      String name,
      LogStorage logStorage,
      DistributedLogstreamPartition distributedLog,
      Subscription writeBufferSubscription,
      int maxBlockSize,
      ActorConditions logStorageAppendConditions) {
    this.name = name;
    this.logStorage = logStorage;
    this.distributedLog = distributedLog;
    this.writeBufferSubscription = writeBufferSubscription;
    this.maxAppendBlockSize = maxBlockSize;
    this.logStorageAppendConditions = logStorageAppendConditions;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  protected void onActorStarting() {

    actor.consume(writeBufferSubscription, this::peekBlock);
  }

  private void peekBlock() {
    if (writeBufferSubscription.peekBlock(blockPeek, maxAppendBlockSize, true) > 0) {
      peekedBlockHandler.run();
    } else {
      actor.yield();
    }
  }

  private void appendBlock() {
    final ByteBuffer rawBuffer = blockPeek.getRawBuffer();
    final MutableDirectBuffer buffer = blockPeek.getBuffer();

    try {
      final byte[] bytes = new byte[rawBuffer.remaining()];
      rawBuffer.get(bytes);

      // Commit position is the position of the last event. DistributedLogstream uses this position
      // to identify duplicate append requests during recovery.
      final long lastEventPosition = getLastEventPosition(bytes);
      final long commitPosition = lastEventPosition;
      distributedLog.append(bytes, commitPosition);
      // TODO: handle errors https://github.com/zeebe-io/zeebe/issues/2064
      blockPeek.markCompleted();
    } catch (Exception e) {
      // try again
      LOG.info("Write failed");
      e.printStackTrace();
    }
  }

  /* Iterate over the events in buffer and find the position of the last event */
  private long getLastEventPosition(byte[] buffer) {
    int bufferOffset = 0;
    final DirectBuffer directBuffer = new UnsafeBuffer(0, 0);

    directBuffer.wrap(buffer);
    long lastEventPosition = -1;

    final LoggedEventImpl nextEvent = new LoggedEventImpl();
    int remaining = buffer.length - bufferOffset;
    while (remaining > 0) {
      nextEvent.wrap(directBuffer, bufferOffset);
      bufferOffset += nextEvent.getFragmentLength();
      lastEventPosition = nextEvent.getPosition();
      remaining = buffer.length - bufferOffset;
    }
    return lastEventPosition;
  }

  private void discardBlock() {
    blockPeek.markFailed();
    // continue with next block
    actor.yield();
  }

  public ActorFuture<Void> close() {
    return actor.close();
  }

  public boolean isFailed() {
    return isFailed.get();
  }

  public long getCurrentAppenderPosition() {
    return writeBufferSubscription.getPosition();
  }
}
