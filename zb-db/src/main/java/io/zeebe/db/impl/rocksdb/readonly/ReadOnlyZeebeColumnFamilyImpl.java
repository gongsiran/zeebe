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

  private static final String UNSUPPORTED_MESSAGE =
      "Read-only column family doesn't support this operation";
  private final long columnFamilyHandle;
  private final ReadOnlyZeebeDbImpl<ColumnFamilyNames> zeebeDb;
  private final ConcurrentLinkedQueue<ZeebeDbReusableBuffers> reusableBuffers =
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
    try (final ZeebeDbReusableBuffers buffers = getReusableBuffers()) {
      buffers.getKey().wrapLong(((DbLong) key).getValue());

      zeebeDb.get(
          columnFamilyHandle, buffers.getKey(), buffers.getKeyBuffer(), buffers.getValueBuffer());

      if (buffers.getValue() != null) {
        buffers.getValue().wrap(buffers.getValueBuffer(), 0, buffers.getValueBuffer().capacity());
        return (ValueType) buffers.getValue();
      }
      return null;
    }
  }

  @Override
  public void whileTrue(KeyValuePairVisitor<KeyType, ValueType> visitor) {
    try (final ZeebeDbReusableBuffers buffers = getReusableBuffers()) {
      zeebeDb.whileTrue(
          columnFamilyHandle,
          (KeyType) buffers.getKey(),
          (ValueType) buffers.getValue(),
          visitor,
          buffers.getKeyBuffer(),
          buffers.getValueBuffer());
    }
  }

  @Override
  public boolean isEmpty() {
    return zeebeDb.isEmpty(columnFamilyHandle);
  }

  @Override
  public boolean exists(KeyType key) {
    try (final ZeebeDbReusableBuffers buffers = getReusableBuffers()) {
      buffers.getKey().wrapLong(((DbLong) key).getValue());
      return zeebeDb.exists(columnFamilyHandle, buffers.getKey(), buffers.getKeyBuffer());
    }
  }

  @Override
  public void forEach(Consumer<ValueType> consumer) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public void forEach(BiConsumer<KeyType, ValueType> consumer) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public void put(KeyType key, ValueType value) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public void delete(KeyType key) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, BiConsumer<KeyType, ValueType> visitor) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, KeyValuePairVisitor<KeyType, ValueType> visitor) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  // TODO: refactor request buffers to contain the logic
  private ZeebeDbReusableBuffers getReusableBuffers() {
    ZeebeDbReusableBuffers buffers = reusableBuffers.poll();

    if (buffers == null) {
      buffers = new ZeebeDbReusableBuffers(reusableBuffers);
    }

    return buffers;
  }
}
