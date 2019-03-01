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
package io.zeebe.db.impl.rocksdb.transaction;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;

class ReadOnlyZeebeColumnFamilyImpl<
        ColumnFamilyNames extends Enum<ColumnFamilyNames>,
        KeyType extends DbKey,
        ValueType extends DbValue>
    implements ColumnFamily<KeyType, ValueType> {

  public static final String UNSUPPORTED_OPERATION_MESSAGE =
      "Read-only column family doesn't support the '%s' operation";
  private final long columnFamilyHandle;

  private final ValueType valueInstance;
  private final KeyType keyInstance;
  private final ReadOnlyZeebeDbImpl<ColumnFamilyNames> zeebeDb;

  ReadOnlyZeebeColumnFamilyImpl(
      ReadOnlyZeebeDbImpl<ColumnFamilyNames> zeebeDb,
      ColumnFamilyNames columnFamily,
      KeyType keyInstance,
      ValueType valueInstance) {
    this.zeebeDb = zeebeDb;
    this.columnFamilyHandle = zeebeDb.getColumnFamilyHandle(columnFamily);
    this.keyInstance = keyInstance;
    this.valueInstance = valueInstance;
  }

  @Override
  public ValueType get(KeyType key) {
    final DirectBuffer valueBuffer = zeebeDb.get(columnFamilyHandle, key);
    if (valueBuffer != null) {
      valueInstance.wrap(valueBuffer, 0, valueBuffer.capacity());
      return valueInstance;
    }
    return null;
  }

  @Override
  public void forEach(Consumer<ValueType> consumer) {
    zeebeDb.foreach(columnFamilyHandle, valueInstance, consumer);
  }

  @Override
  public void forEach(BiConsumer<KeyType, ValueType> consumer) {
    zeebeDb.foreach(columnFamilyHandle, keyInstance, valueInstance, consumer);
  }

  @Override
  public void whileTrue(KeyValuePairVisitor<KeyType, ValueType> visitor) {
    zeebeDb.whileTrue(columnFamilyHandle, keyInstance, valueInstance, visitor);
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, BiConsumer<KeyType, ValueType> visitor) {
    zeebeDb.whileEqualPrefix(columnFamilyHandle, keyPrefix, keyInstance, valueInstance, visitor);
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, KeyValuePairVisitor<KeyType, ValueType> visitor) {
    zeebeDb.whileEqualPrefix(columnFamilyHandle, keyPrefix, keyInstance, valueInstance, visitor);
  }

  @Override
  public boolean exists(KeyType key) {
    return zeebeDb.exists(columnFamilyHandle, key);
  }

  @Override
  public void put(KeyType key, ValueType value) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_OPERATION_MESSAGE, "put"));
  }

  @Override
  public void delete(KeyType key) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_OPERATION_MESSAGE, "delete"));
  }

  @Override
  public boolean isEmpty() {
    return zeebeDb.isEmpty(columnFamilyHandle);
  }
}
