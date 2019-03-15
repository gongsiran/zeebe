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

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.rocksdb.DbContext;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateSnapshotMetadata;
import io.zeebe.logstreams.state.StateStorage;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Block index, mapping an event's position to the physical address of the block in which it resides
 * in storage.
 *
 * <p>Each Event has a position inside the stream. This position addresses it uniquely and is
 * assigned when the entry is first published to the stream. The position never changes and is
 * preserved through maintenance operations like compaction.
 *
 * <p>In order to read an event, the position must be translated into the "physical address" of the
 * block in which it resides in storage. Then, the block can be scanned for the event position
 * requested.
 */
public class LogBlockIndex implements SnapshotSupport {
  public static final int VALUE_NOT_FOUND = -1;

  private final StateSnapshotController stateSnapshotController;
  private final DbContext dbContext;
  private ZeebeDb db;

  private ColumnFamily<DbLong, DbLong> blockPositionToAddress;
  private final DbLong blockPosition = new DbLong();
  private final DbLong blockAddress = new DbLong();

  private long lastVirtualPosition = VALUE_NOT_FOUND;

  /**
   * Creates an instance of a LogBlockIndex for a given DB (multiple block indexes can be
   * instantiated for the same block index DB). Note that before the block index can be used, {@link
   * #openDb() openDb()} method must be called. after any recovery actions
   *
   * @param dbFactory the factory used to obtain the DB
   * @param stateStorage DB runtime and snapshot location
   */
  public LogBlockIndex(
      DbContext dbContext,
      ZeebeDbFactory<LogBlockColumnFamilies> dbFactory,
      StateStorage stateStorage) {
    this.dbContext = dbContext;
    this.stateSnapshotController = new StateSnapshotController(dbFactory, stateStorage);
  }

  /**
   * Opens and initializes the DB. If there is a snapshot to recover from, {@link
   * #recoverFromSnapshot()} recoverFromSnapshot()} should be called before this method
   */
  public void openDb() {
    db = stateSnapshotController.openDb();
    dbContext.setTransactionProvider(db::getTransaction);
    blockPositionToAddress =
        db.createColumnFamily(
            dbContext, LogBlockColumnFamilies.BLOCK_POSITION_ADDRESS, blockPosition, blockAddress);
  }

  public void closeDb() throws Exception {
    if (db != null) {
      stateSnapshotController.close();
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
    final long blockPosition = lookupBlockPosition(entryPosition);
    if (blockPosition == VALUE_NOT_FOUND) {
      return VALUE_NOT_FOUND;
    }

    this.blockPosition.wrapLong(blockPosition);
    final DbLong address = blockPositionToAddress.get(this.blockPosition);

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
    final AtomicLong blockPosition = new AtomicLong(VALUE_NOT_FOUND);

    blockPositionToAddress.whileTrue(
        (key, val) -> {
          final long currentBlockPosition = key.getValue();

          if (currentBlockPosition <= entryPosition) {
            blockPosition.set(currentBlockPosition);
            return true;
          } else {
            return false;
          }
        });

    return blockPosition.get();
  }

  public void addBlock(long blockPosition, long blockAddress) {
    if (lastVirtualPosition >= blockPosition) {
      final String errorMessage =
          String.format(
              "Illegal value for position.Value=%d, last value in index=%d. Must provide positions in ascending order.",
              blockPosition, lastVirtualPosition);
      throw new IllegalArgumentException(errorMessage);
    }

    lastVirtualPosition = blockPosition;
    this.blockPosition.wrapLong(blockPosition);
    this.blockAddress.wrapLong(blockAddress);

    blockPositionToAddress.put(this.blockPosition, this.blockAddress);
  }

  @Override
  public void writeSnapshot(final long snapshotEventPosition) {
    final StateSnapshotMetadata snapshotMetadata = new StateSnapshotMetadata(snapshotEventPosition);
    stateSnapshotController.takeSnapshot(snapshotMetadata);
  }

  @Override
  public void recoverFromSnapshot() throws Exception {
    final StateSnapshotMetadata snapshotMetadata =
        stateSnapshotController.recoverFromLatestSnapshot();
    lastVirtualPosition = snapshotMetadata.getLastWrittenEventPosition();
  }

  public boolean isEmpty() {
    return blockPositionToAddress.isEmpty();
  }

  /**
   * The last position written by this block index wrapper.
   *
   * @return last position written by this wrapper, -1 if none was written
   */
  public long getLastPosition() {
    return lastVirtualPosition;
  }
}
