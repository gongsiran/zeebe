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
package io.zeebe.logstreams.impl.log.index;

import io.zeebe.db.ReadOnlyColumnFamily;
import io.zeebe.db.ReadOnlyZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DbLong;
import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

// TODO: rewrite documentation
/**
 * Read-only block index, mapping an event's position to the physical address of the block in which
 * it resides in storage.
 *
 * <p>Classes that extend the read-only log block index should still assign
 *
 * <p>Each Event has a position inside the stream. This position addresses it uniquely and is
 * assigned when the entry is first published to the stream. The position never changes and is
 * preserved through maintenance operations like compaction.
 *
 * <p>In order to read an event, the position must be translated into the "physical address" of the
 * block in which it resides in storage. Then, the block can be scanned for the event position
 * requested.
 */
public class ReadOnlyLogBlockIndex {
  protected ReadOnlyZeebeDb<LogBlockColumnFamilies> db;

  protected ReadOnlyColumnFamily<DbLong, DbLong> blockPositionToAddress;
  protected final DbLong blockPosition = new DbLong();
  protected final DbLong blockAddress = new DbLong();
  private final ConcurrentLinkedQueue<ReadOnlyRequestBuffers> requestBuffers =
      new ConcurrentLinkedQueue();

  public ReadOnlyLogBlockIndex() {}

  public ReadOnlyLogBlockIndex(
      ZeebeDbFactory<LogBlockColumnFamilies> dbFactory, File runtimeDirectory) {
    db = dbFactory.createReadOnlyDb(runtimeDirectory);
    blockPositionToAddress =
        db.createColumnFamily(
            LogBlockColumnFamilies.BLOCK_POSITION_ADDRESS, blockPosition, blockAddress);
  }

  public void closeDb() throws Exception {
    if (db != null) {
      db.close();
      db = null;
    }
  }

  /**
   * Returns the physical address of the block in which the log entry identified by the provided
   * position resides.
   *
   * @param entryPosition a virtual log position
   * @return the physical address of the block containing the log entry identified by the provided
   *     virtual position
   */
  public long lookupBlockAddress(final long entryPosition) {
    try (final ReadOnlyRequestBuffers requestBuffers = getRequestBuffers()) {
      return lookupBlockAddress(requestBuffers);
    }
  }

  public long lookupBlockAddress(final ReadOnlyRequestBuffers buffers) {
    final long blockPosition = lookupBlockPosition(buffers);
    if (blockPosition == -1) {
      return -1;
    }

    buffers.key.wrapLong(blockPosition);
    final DbLong address =
        blockPositionToAddress.get(
            buffers.key, buffers.value, buffers.keyBuffer, buffers.valueBuffer);

    return address != null ? address.getValue() : -1;
  }

  /**
   * Returns the position of the first log entry of the the block in which the log entry identified
   * by the provided position resides.
   *
   * @param entryPosition a virtual log position
   * @return the position of the block containing the log entry identified by the provided virtual
   *     position
   */
  public long lookupBlockPosition(final long entryPosition) {
    try (final ReadOnlyRequestBuffers requestBuffers = getRequestBuffers()) {
      return lookupBlockPosition(requestBuffers);
    }
  }

  private long lookupBlockPosition(final ReadOnlyRequestBuffers buffers) {
    final AtomicLong largestBlockPosition = new AtomicLong(-1);

    blockPositionToAddress.whileTrue(
        (iterKey, iterValue) -> {
          final long currentBlockPosition = iterKey.getValue();

          if (currentBlockPosition <= buffers.key.getValue()) {
            largestBlockPosition.set(currentBlockPosition);
            return true;
          } else {
            return false;
          }
        },
        buffers.key,
        buffers.value,
        buffers.keyBuffer,
        buffers.valueBuffer);

    return blockPosition.getValue();
  }

  public boolean isEmpty() {
    return blockPositionToAddress.isEmpty();
  }

  private ReadOnlyRequestBuffers getRequestBuffers() {
    ReadOnlyRequestBuffers buffers = requestBuffers.poll();

    if (buffers == null) {
      buffers = new ReadOnlyRequestBuffers(requestBuffers);
    }

    return buffers;
  }
}
