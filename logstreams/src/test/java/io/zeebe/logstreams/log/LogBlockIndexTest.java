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
package io.zeebe.logstreams.log;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.impl.log.index.LogBlockColumnFamilies;
import io.zeebe.logstreams.impl.log.index.LogBlockIndex;
import io.zeebe.logstreams.state.StateStorage;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class LogBlockIndexTest {

  public static final int ADDRESS_MULTIPLIER = 200;
  public static final int ENTRY_OFFSET = 5;

  private LogBlockIndex blockIndex;
  private DbLong entryPosition = new DbLong();
  private DbLong blockAddress = new DbLong();
  private ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private DirectBuffer valueViewBuffer = new UnsafeBuffer();

  @Rule public ExpectedException exception = ExpectedException.none();
  @Rule public TemporaryFolder runtimeDirectory = new TemporaryFolder();
  @Rule public TemporaryFolder snapshotDirectory = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    runtimeDirectory.create();
    snapshotDirectory.create();

    startBlockIndexDb();
  }

  private void startBlockIndexDb() throws Exception {
    final ZeebeDbFactory<LogBlockColumnFamilies> dbFactory =
        ZeebeRocksDbFactory.newFactory(LogBlockColumnFamilies.class);

    final StateStorage stateStorage =
        new StateStorage(runtimeDirectory.getRoot(), snapshotDirectory.getRoot());

    blockIndex = new LogBlockIndex(dbFactory, stateStorage);
    blockIndex.recoverFromSnapshot();
    blockIndex.openDb();
  }

  @After
  public void tearDown() {
    runtimeDirectory.delete();
    snapshotDirectory.delete();

    try {
      blockIndex.closeDb();
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void shouldAddBlocks() {
    final int numBlocks = 10;

    // when
    final long lastPosition = addBlocks(numBlocks);

    // then
    lookupAndAssert(numBlocks);
    assertThat(blockIndex.isEmpty()).isFalse();
    assertThat(blockIndex.getLastPosition()).isEqualTo(lastPosition);
  }

  @Test
  public void shouldNotAddBlockWithEqualPos() {
    // given
    entryPosition.wrapLong(10);
    blockAddress.wrapLong(0);
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);

    // then
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Illegal value for position");

    // when
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);
  }

  @Test
  public void shouldNotAddBlockWithSmallerPos() {
    // given
    entryPosition.wrapLong(10);
    blockAddress.wrapLong(0);
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);

    // then
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Illegal value for position");

    // when
    entryPosition.wrapLong(10);
    blockAddress.wrapLong(0);
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);
  }

  @Test
  public void shouldReturnMinusOneForEmptyBlockIndex() {
    entryPosition.wrapLong(-1);
    assertThat(
            blockIndex.lookupBlockAddress(entryPosition, blockAddress, keyBuffer, valueViewBuffer))
        .isEqualTo(-1);
    entryPosition.wrapLong(1);
    assertThat(
            blockIndex.lookupBlockAddress(entryPosition, blockAddress, keyBuffer, valueViewBuffer))
        .isEqualTo(-1);
  }

  @Test
  public void shouldNotReturnFirstBlockAddress() {
    // given
    entryPosition.wrapLong(10);
    blockAddress.wrapLong(1000);
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);

    // then
    for (int i = 0; i < 10; i++) {
      entryPosition.wrapLong(i);
      assertThat(
              blockIndex.lookupBlockAddress(
                  entryPosition, blockAddress, keyBuffer, valueViewBuffer))
          .isEqualTo(-1);
    }
  }

  @Test
  public void shouldReturnFirstBlockAddress() {
    // given
    entryPosition.wrapLong(10);
    blockAddress.wrapLong(1000);
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);

    // then
    for (int i = 10; i < 100; i++) {
      entryPosition.wrapLong(i);
      assertThat(blockIndex.lookupBlockAddress(entryPosition, blockAddress, keyBuffer, valueViewBuffer))
          .isEqualTo(1000);
    }
  }

  @Test
  public void shouldLookupBlockAddresses() {
    final int capacity = 100;

    // given

    for (int i = 0; i < capacity; i++) {
      final int pos = (i + 1) * 10;
      final int addr = (i + 1) * 100;

      entryPosition.wrapLong(pos);
      blockAddress.wrapLong(addr);
      blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);
    }

    // then

    for (int i = 0; i < capacity; i++) {
      final int expectedAddr = (i + 1) * 100;

      for (int j = 0; j < 10; j++) {
        final int pos = ((i + 1) * 10) + j;
        entryPosition.wrapLong(pos);
        assertThat(
                blockIndex.lookupBlockAddress(entryPosition, blockAddress, keyBuffer, valueViewBuffer))
            .isEqualTo(expectedAddr);
      }
    }
  }

  @Test
  public void shouldNotReturnFirstBlockPosition() {
    // given
    entryPosition.wrapLong(10);
    blockAddress.wrapLong(1000);
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);

    // then
    for (int i = 0; i < 10; i++) {
      entryPosition.wrapLong(i);
      assertThat(blockIndex.lookupBlockPosition(entryPosition, blockAddress)).isEqualTo(-1);
    }
  }

  @Test
  public void shouldReturnFirstBlockPosition() {
    // given
    entryPosition.wrapLong(10);
    blockAddress.wrapLong(1000);
    blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);

    // then
    for (int i = 10; i < 100; i++) {
      entryPosition.wrapLong(i);
      assertThat(blockIndex.lookupBlockPosition(entryPosition, blockAddress)).isEqualTo(10);
    }
  }

  @Test
  public void shouldLookupBlockPositions() {
    final int capacity = 100;

    // given

    for (int i = 0; i < capacity; i++) {
      final int position = (i + 1) * 10;
      final int address = (i + 1) * 100;

      entryPosition.wrapLong(position);
      blockAddress.wrapLong(address);
      blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);
    }

    // then

    for (int i = 0; i < capacity; i++) {
      final int expectedPosition = (i + 1) * 10;

      for (int j = 0; j < 10; j++) {
        final int position = ((i + 1) * 10) + j;

        entryPosition.wrapLong(position);
        assertThat(blockIndex.lookupBlockPosition(entryPosition, blockAddress))
            .isEqualTo(expectedPosition);
      }
    }
  }

  @Test
  public void shouldRecoverIndexFromSnapshot() throws Exception {
    // given
    final int numBlocks = 10;
    final long snapshotPosition = addBlocks(numBlocks);
    blockIndex.writeSnapshot(snapshotPosition);

    // when
    blockIndex.closeDb(); // close and reopen DB
    blockIndex.recoverFromSnapshot();
    startBlockIndexDb();

    // then
    lookupAndAssert(numBlocks);
    assertThat(blockIndex.getLastPosition()).isEqualTo(snapshotPosition);
  }

  // Adds blocks and returns the last added position
  private long addBlocks(int numBlocks) {
    for (int blockPos = 0; blockPos < numBlocks * ENTRY_OFFSET; blockPos += ENTRY_OFFSET) {
      final int address = blockPos * ADDRESS_MULTIPLIER;
      entryPosition.wrapLong(blockPos);
      blockAddress.wrapLong(address);
      blockIndex.addBlock(entryPosition, blockAddress, keyBuffer, valueBuffer);
    }

    return (numBlocks - 1) * ENTRY_OFFSET;
  }

  private void lookupAndAssert(int numBlocks) {
    for (int blockPos = 0; blockPos < numBlocks * ENTRY_OFFSET; blockPos += ENTRY_OFFSET) {
      final int address = blockPos * ADDRESS_MULTIPLIER;

      for (int entryPos = blockPos; entryPos < blockPos + ENTRY_OFFSET; entryPos++) {
        entryPosition.wrapLong(entryPos);
        assertThat(blockIndex.lookupBlockPosition(entryPosition, blockAddress)).isEqualTo(blockPos);
        assertThat(
                blockIndex.lookupBlockAddress(
                    entryPosition, blockAddress, valueBuffer, valueViewBuffer))
            .isEqualTo(address);
      }
    }
  }
}
