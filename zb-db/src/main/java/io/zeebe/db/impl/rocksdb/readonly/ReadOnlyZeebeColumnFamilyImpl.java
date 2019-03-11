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
package io.zeebe.db.impl.rocksdb.readonly;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import io.zeebe.db.impl.DbLong;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class ReadOnlyZeebeColumnFamilyImpl<
        ColumnFamilyNames extends Enum<ColumnFamilyNames>,
        KeyType extends DbKey,
        ValueType extends DbValue>
    implements ColumnFamily<KeyType, ValueType> {

  private final long columnFamilyHandle;
  private final ReadOnlyZeebeDbImpl<ColumnFamilyNames> zeebeDb;
  private final ConcurrentLinkedQueue<ReadOnlyRequestBuffers> requestBuffers =
      new ConcurrentLinkedQueue();

  ReadOnlyZeebeColumnFamilyImpl(
      ReadOnlyZeebeDbImpl<ColumnFamilyNames> zeebeDb,
      ColumnFamilyNames columnFamily,
      KeyType keyInstance,
      ValueType valueInstance) {
    this.zeebeDb = zeebeDb;
    this.columnFamilyHandle = zeebeDb.getColumnFamilyHandle(columnFamily);
  }

  @Override
  public ValueType get(KeyType key) {
    try (final ReadOnlyRequestBuffers buffers = getRequestBuffers()) {
      buffers.key.wrapLong(((DbLong) key).getValue());

      zeebeDb.get(columnFamilyHandle, buffers.key, buffers.keyBuffer, buffers.valueBuffer);

      if (buffers.value != null) {
        buffers.value.wrap(buffers.valueBuffer, 0, buffers.valueBuffer.capacity());
        return (ValueType) buffers.value;
      }
      return null;
    }
  }

  @Override
  public void whileTrue(KeyValuePairVisitor<KeyType, ValueType> visitor) {
    try (final ReadOnlyRequestBuffers buffers = getRequestBuffers()) {
      zeebeDb.whileTrue(
          columnFamilyHandle,
          (KeyType) buffers.key,
          (ValueType) buffers.value,
          visitor,
          buffers.keyBuffer,
          buffers.valueBuffer);
    }
  }

  @Override
  public boolean isEmpty() {
    return zeebeDb.isEmpty(columnFamilyHandle);
  }

  @Override
  public boolean exists(KeyType key) {
    try (final ReadOnlyRequestBuffers buffers = getRequestBuffers()) {
      buffers.key.wrapLong(((DbLong) key).getValue());
      return zeebeDb.exists(columnFamilyHandle, buffers.key);
    }
  }

  @Override
  public void forEach(Consumer<ValueType> consumer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void forEach(BiConsumer<KeyType, ValueType> consumer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(KeyType key, ValueType value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(KeyType key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, BiConsumer<KeyType, ValueType> visitor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, KeyValuePairVisitor<KeyType, ValueType> visitor) {
    throw new UnsupportedOperationException();
  }

  // TODO: refactor request buffers to contain the logic
  private ReadOnlyRequestBuffers getRequestBuffers() {
    ReadOnlyRequestBuffers buffers = requestBuffers.poll();

    if (buffers == null) {
      buffers = new ReadOnlyRequestBuffers(requestBuffers);
    }

    return buffers;
  }
}
