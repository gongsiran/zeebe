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
import io.zeebe.db.ReadOnlyZeebeDb;
import io.zeebe.db.impl.rocksdb.RocksDbIterator;
import io.zeebe.db.impl.rocksdb.RocksDbReadOptions;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;

public class ReadOnlyZeebeDbImpl<ColumnFamilyNames extends Enum<ColumnFamilyNames>> extends RocksDB
    implements ReadOnlyZeebeDb {
  private static final Field NATIVE_HANDLE_FIELD;
  public static final String UNSUPPORTED_OPERATION_MESSAGE =
      "Read-only zeebe DB doesn't support the '%s' operation";

  static {
    RocksDB.loadLibrary();

    try {
      NATIVE_HANDLE_FIELD = RocksObject.class.getDeclaredField("nativeHandle_");
      NATIVE_HANDLE_FIELD.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  public static final byte[] ZERO_SIZE_ARRAY = new byte[0];
  private final RocksDB readOnlyDb;
  private final EnumMap<ColumnFamilyNames, Long> columnFamilyMap;
  private final Long2ObjectHashMap<ColumnFamilyHandle> handleToEnumMap;
  private final List<AutoCloseable> closeables;
  private final Class<ColumnFamilyNames> columnFamilyTypeClass;
  private final ReadOptions readOptions = new ReadOptions();
  private StringBuilder keyMayExistStringBuilder = new StringBuilder();

  // key/value buffers/views
  protected final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  protected final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();

  protected final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  protected final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);
  private MutableDirectBuffer prefixKeyBuffer;

  private boolean activePrefixIteration;

  public static <ColumnFamilyNames extends Enum<ColumnFamilyNames>>
      ReadOnlyZeebeDbImpl<ColumnFamilyNames> openReadOnlyDb(
          final DBOptions options,
          final String path,
          final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
          final List<AutoCloseable> closeables,
          Class<ColumnFamilyNames> columnFamilyTypeClass)
          throws RocksDBException {
    final EnumMap<ColumnFamilyNames, Long> columnFamilyMap = new EnumMap<>(columnFamilyTypeClass);

    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    final RocksDB readOnlyDb =
        RocksDB.openReadOnly(options, path, columnFamilyDescriptors, handles);

    final ColumnFamilyNames[] enumConstants = columnFamilyTypeClass.getEnumConstants();
    final Long2ObjectHashMap<ColumnFamilyHandle> handleToEnumMap = new Long2ObjectHashMap<>();
    for (int i = 0; i < handles.size(); i++) {
      columnFamilyMap.put(enumConstants[i], getNativeHandle(handles.get(i)));
      handleToEnumMap.put(getNativeHandle(handles.get(i)), handles.get(i));
    }

    final ReadOnlyZeebeDbImpl<ColumnFamilyNames> db =
        new ReadOnlyZeebeDbImpl<>(
            readOnlyDb, columnFamilyMap, handleToEnumMap, closeables, columnFamilyTypeClass);

    return db;
  }

  public ReadOnlyZeebeDbImpl(
      RocksDB readOnlyDb,
      EnumMap<ColumnFamilyNames, Long> columnFamilyMap,
      Long2ObjectHashMap<ColumnFamilyHandle> handleToEnumMap,
      List<AutoCloseable> closeables,
      Class<ColumnFamilyNames> columnFamilyTypeClass) {
    super(getNativeHandle(readOnlyDb));

    this.readOnlyDb = readOnlyDb;
    this.columnFamilyMap = columnFamilyMap;
    this.handleToEnumMap = handleToEnumMap;
    this.closeables = closeables;
    this.columnFamilyTypeClass = columnFamilyTypeClass;
  }

  @Override
  public ColumnFamily createColumnFamily(
      Enum columnFamily, DbKey keyInstance, DbValue valueInstance) {
    return new ReadOnlyZeebeColumnFamilyImpl(this, columnFamily, keyInstance, valueInstance);
  }

  private static long getNativeHandle(final RocksObject object) {
    try {
      return (long) NATIVE_HANDLE_FIELD.get(object);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Unexpected error occurred trying to access private nativeHandle_ field", e);
    }
  }

  protected long getColumnFamilyHandle(ColumnFamilyNames columnFamily) {
    return columnFamilyMap.get(columnFamily);
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// GET ///////////////////////////////////
  ////////////////////////////////////////////////////////////////////

  protected DirectBuffer get(
      long columnFamilyHandle,
      DbKey key,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer valueBuffer) {
    key.write(keyBuffer, 0);
    final int keyLength = key.getLength();
    return getValue(columnFamilyHandle, keyLength, keyBuffer, valueBuffer);
  }

  private DirectBuffer getValue(
      long columnFamilyHandle,
      int keyLength,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer valueBuffer) {

    final int valueLength = valueBuffer.capacity();
    try {
      final int readBytes =
          get(
              getNativeHandle(readOnlyDb),
              keyBuffer.byteArray(),
              0,
              keyLength,
              valueBuffer.byteArray(),
              0,
              valueLength,
              columnFamilyHandle);

      if (readBytes >= valueLength) {
        valueBuffer.checkLimit(readBytes);
        return getValue(columnFamilyHandle, keyLength, keyBuffer, valueBuffer); // TODO: check
      } else if (readBytes <= RocksDB.NOT_FOUND) {
        return null;
      } else {
        // valueViewBuffer.wrap(this.valueBuffer, 0, readBytes);
        return valueBuffer;
      }
    } catch (RocksDBException e) {
      throw new RuntimeException("Unexpected error trying to read RocksDB entry", e);
    }
  }

  protected boolean exists(long columnFamilyHandle, DbKey key) {
    key.write(keyBuffer, 0);

    if (!keyMayExist(
        nativeHandle_,
        keyBuffer.byteArray(),
        0,
        key.getLength(),
        columnFamilyHandle,
        keyMayExistStringBuilder)) {
      return false;
    }

    // to read not the value
    final int zeroLength = 0;
    try {
      final int readBytes =
          get(
              nativeHandle_,
              keyBuffer.byteArray(),
              0,
              key.getLength(),
              valueBuffer.byteArray(),
              0,
              zeroLength,
              columnFamilyHandle);

      return readBytes != NOT_FOUND;
    } catch (RocksDBException e) {
      throw new RuntimeException("Unexpected error occurred trying to read RocksDB entry", e);
    }
  }

  ////////////////////////////////////////////////////////////////////
  //////////////////////////// ITERATION /////////////////////////////
  ////////////////////////////////////////////////////////////////////

  public RocksDbIterator newIterator(long columnFamilyHandle) {
    return new RocksDbIterator(readOnlyDb, iteratorCF(nativeHandle_, columnFamilyHandle));
  }

  public RocksDbIterator newIterator(long columnFamilyHandle, RocksDbReadOptions options) {
    return new RocksDbIterator(
        this, iteratorCF(nativeHandle_, columnFamilyHandle, options.getNativeHandle()));
  }

  public <KeyType extends DbKey, ValueType extends DbValue> void whileTrue(
      long columnFamilyHandle,
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> visitor,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer valueBuffer) {

    try (final RocksDbIterator iterator = newIterator(columnFamilyHandle)) {
      boolean shouldVisitNext = true;
      for (iterator.seekToFirst(); iterator.isValid() && shouldVisitNext; iterator.next()) {
        shouldVisitNext =
            visit(keyInstance, valueInstance, visitor, iterator, keyBuffer, valueBuffer);
      }
    }
  }

  private <KeyType extends DbKey, ValueType extends DbValue> boolean visit(
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> iteratorConsumer,
      RocksDbIterator iterator,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer valueBuffer) {
    keyBuffer.putBytes(0, iterator.key(), 0, iterator.key().length);
    valueBuffer.putBytes(0, iterator.value(), 0, iterator.value().length);

    keyInstance.wrap(keyBuffer, 0, keyBuffer.capacity());
    valueInstance.wrap(valueBuffer, 0, valueBuffer.capacity());

    return iteratorConsumer.visit(keyInstance, valueInstance);
  }

  public boolean isEmpty(long columnFamilyHandle) {
    try (final RocksDbIterator iterator = newIterator(columnFamilyHandle)) {
      iterator.seekToFirst();
      final boolean hasEntry = iterator.isValid();

      return !hasEntry;
    }
  }

  @Override
  public void close() {
    closeables.forEach(
        closable -> {
          try {
            closable.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    //    super.close();
  }
}
