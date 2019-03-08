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

import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateSnapshotMetadata;
import io.zeebe.logstreams.state.StateStorage;

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
public class LogBlockIndex extends ReadOnlyLogBlockIndex implements SnapshotSupport {
  private final StateSnapshotController stateSnapshotController;
  private long lastVirtualPosition = -1;

  public LogBlockIndex(
      ZeebeDbFactory<LogBlockColumnFamilies> dbFactory, StateStorage stateStorage) {
    this.stateSnapshotController = new StateSnapshotController(dbFactory, stateStorage);
  }

  public void openDb() {
    db = stateSnapshotController.openDb();
    blockPositionToAddress =
        db.createColumnFamily(
            LogBlockColumnFamilies.BLOCK_POSITION_ADDRESS, blockPosition, blockAddress);
  }

  public void closeDb() throws Exception {
    if (db != null) {
      stateSnapshotController.close();
      db = null;
    }
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

  public long getLastPosition() {
    return lastVirtualPosition;
  }
}
