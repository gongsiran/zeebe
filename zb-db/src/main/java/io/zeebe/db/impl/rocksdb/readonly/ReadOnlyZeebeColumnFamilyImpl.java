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

import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import io.zeebe.db.ReadOnlyColumnFamily;
import org.agrona.ExpandableArrayBuffer;

class ReadOnlyZeebeColumnFamilyImpl<
        ColumnFamilyNames extends Enum<ColumnFamilyNames>,
        KeyType extends DbKey,
        ValueType extends DbValue>
    implements ReadOnlyColumnFamily<KeyType, ValueType> {

  private final long columnFamilyHandle;
  private final ReadOnlyZeebeDbImpl<ColumnFamilyNames> zeebeDb;

  ReadOnlyZeebeColumnFamilyImpl(
      ReadOnlyZeebeDbImpl<ColumnFamilyNames> zeebeDb,
      ColumnFamilyNames columnFamily,
      KeyType keyInstance,
      ValueType valueInstance) {
    this.zeebeDb = zeebeDb;
    this.columnFamilyHandle = zeebeDb.getColumnFamilyHandle(columnFamily);
  }

  @Override
  public ValueType get(
      KeyType key,
      ValueType value,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer valueBuffer) {
    zeebeDb.get(columnFamilyHandle, key, keyBuffer, valueBuffer);
    if (valueBuffer != null) {
      value.wrap(valueBuffer, 0, valueBuffer.capacity());
      return value;
    }
    return null;
  }

  @Override
  public void whileTrue(
      KeyValuePairVisitor<KeyType, ValueType> visitor,
      KeyType key,
      ValueType value,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer valueBuffer) {
    zeebeDb.whileTrue(columnFamilyHandle, key, value, visitor, keyBuffer, valueBuffer);
  }

  @Override
  public boolean isEmpty() {
    return zeebeDb.isEmpty(columnFamilyHandle);
  }

  @Override
  public boolean exists(KeyType key) {
    return zeebeDb.exists(columnFamilyHandle, key);
  }
}
