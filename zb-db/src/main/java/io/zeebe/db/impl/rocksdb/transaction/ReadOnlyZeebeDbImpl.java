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

import static io.zeebe.util.buffer.BufferUtil.startsWith;

import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import io.zeebe.db.TransactionOperation;
import io.zeebe.db.ZeebeDb;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    implements AutoCloseable, ZeebeDb {
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
  private final ReadOptions readOptions = new ReadOptions(); // TODO: check readOptions
  private StringBuilder keyMayExistStringBuilder = new StringBuilder();

  // key/value buffers/views
  protected final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  protected final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();

  protected final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  protected final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);
  private MutableDirectBuffer prefixKeyBuffer;

  // TODO check
  // buffers used inside the batch
  private final ExpandableArrayBuffer keyBatchBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBatchBuffer = new ExpandableArrayBuffer();

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
    final Long2ObjectHashMap<ColumnFamilyHandle> handleToEnumMap = new Long2ObjectHashMap();
    for (int i = 0; i < handles.size(); i++) {
      columnFamilyMap.put(enumConstants[i], getNativeHandle(handles.get(i)));
      handleToEnumMap.put(getNativeHandle(handles.get(i)), handles.get(i));
    }

    final ReadOnlyZeebeDbImpl<ColumnFamilyNames> db =
        new ReadOnlyZeebeDbImpl<>(
            readOnlyDb, columnFamilyMap, handleToEnumMap, closeables, columnFamilyTypeClass);

    return db;
  }

  // TODO: allow user to pass buffers

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

  public ReadOnlyZeebeColumnFamilyImpl createColumnFamily(
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

  protected DirectBuffer get(long columnFamilyHandle, DbKey key) {
    key.write(keyBuffer, 0);
    final int keyLength = key.getLength();
    return getValue(columnFamilyHandle, keyLength);
  }

  private DirectBuffer getValue(long columnFamilyHandle, int keyLength) {
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
        return getValue(columnFamilyHandle, keyLength);
      } else if (readBytes <= RocksDB.NOT_FOUND) {
        return null;
      } else {
        valueViewBuffer.wrap(valueBuffer, 0, readBytes);
        return valueViewBuffer;
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

  public <ValueType extends DbValue> void foreach(
      long columnFamilyHandle, ValueType iteratorValue, Consumer<ValueType> consumer) {
    foreach(
        columnFamilyHandle,
        (keyBuffer, valueBuffer) -> {
          iteratorValue.wrap(valueBuffer, 0, valueBuffer.capacity());
          consumer.accept(iteratorValue);
        });
  }

  public <KeyType extends DbKey, ValueType extends DbValue> void foreach(
      long columnFamilyHandle,
      KeyType iteratorKey,
      ValueType iteratorValue,
      BiConsumer<KeyType, ValueType> consumer) {
    foreach(
        columnFamilyHandle,
        (keyBuffer, valueBuffer) -> {
          iteratorKey.wrap(keyBuffer, 0, keyBuffer.capacity());
          iteratorValue.wrap(valueBuffer, 0, valueBuffer.capacity());
          consumer.accept(iteratorKey, iteratorValue);
        });
  }

  private void foreach(
      long columnFamilyHandle, BiConsumer<DirectBuffer, DirectBuffer> keyValuePairConsumer) {
    try (final RocksDbIterator iterator = newIterator(columnFamilyHandle)) {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        keyViewBuffer.wrap(iterator.key());
        valueViewBuffer.wrap(iterator.value());
        keyValuePairConsumer.accept(keyViewBuffer, valueViewBuffer);
      }
    }
  }

  public <KeyType extends DbKey, ValueType extends DbValue> void whileTrue(
      long columnFamilyHandle,
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> visitor) {

    try (final RocksDbIterator iterator = newIterator(columnFamilyHandle)) {
      boolean shouldVisitNext = true;
      for (iterator.seekToFirst(); iterator.isValid() && shouldVisitNext; iterator.next()) {
        shouldVisitNext = visit(keyInstance, valueInstance, visitor, iterator);
      }
    }
  }

  protected <KeyType extends DbKey, ValueType extends DbValue> void whileEqualPrefix(
      long columnFamilyHandle,
      DbKey prefix,
      KeyType keyInstance,
      ValueType valueInstance,
      BiConsumer<KeyType, ValueType> visitor) {
    whileEqualPrefix(
        columnFamilyHandle,
        prefix,
        keyInstance,
        valueInstance,
        (k, v) -> {
          visitor.accept(k, v);
          return true;
        });
  }

  /**
   * NOTE: it doesn't seem possible in Java RocksDB to set a flexible prefix extractor on iterators
   * at the moment, so using prefixes seem to be mostly related to skipping files that do not
   * contain keys with the given prefix (which is useful anyway), but it will still iterate over all
   * keys contained in those files, so we still need to make sure the key actually matches the
   * prefix.
   *
   * <p>While iterating over subsequent keys we have to validate it.
   */
  protected <KeyType extends DbKey, ValueType extends DbValue> void whileEqualPrefix(
      long columnFamilyHandle,
      DbKey prefix,
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> visitor) {
    if (activePrefixIteration) {
      throw new IllegalStateException(
          "Currently nested prefix iterations are not supported! This will cause unexpected behavior.");
    }

    activePrefixIteration = true;
    try (final RocksDbReadOptions options =
            new RocksDbReadOptions().setPrefixSameAsStart(true).setTotalOrderSeek(false);
        final RocksDbIterator iterator = newIterator(columnFamilyHandle, options)) {
      prefix.write(prefixKeyBuffer, 0);
      final int prefixLength = prefix.getLength();

      boolean shouldVisitNext = true;
      for (iterator.seek(prefixKeyBuffer.byteArray(), prefixLength);
          iterator.isValid() && shouldVisitNext;
          iterator.next()) {
        final byte[] keyBytes = iterator.key();
        if (startsWith(
            prefixKeyBuffer.byteArray(), 0, prefix.getLength(), keyBytes, 0, keyBytes.length)) {
          shouldVisitNext = visit(keyInstance, valueInstance, visitor, iterator);
        }
      }
    } finally {
      activePrefixIteration = false;
    }
  }

  private <KeyType extends DbKey, ValueType extends DbValue> boolean visit(
      KeyType keyInstance,
      ValueType valueInstance,
      KeyValuePairVisitor<KeyType, ValueType> iteratorConsumer,
      RocksDbIterator iterator) {
    keyViewBuffer.wrap(iterator.key());
    valueViewBuffer.wrap(iterator.value());

    keyInstance.wrap(keyViewBuffer, 0, keyViewBuffer.capacity());
    valueInstance.wrap(valueViewBuffer, 0, valueViewBuffer.capacity());

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

    super.close();
  }

  @Override
  public void transaction(TransactionOperation operations) {
    throw new UnsupportedOperationException(
        String.format(UNSUPPORTED_OPERATION_MESSAGE, "transaction"));
  }

  @Override
  public void createSnapshot(File snapshotDir) {
    throw new UnsupportedOperationException(
        String.format(UNSUPPORTED_OPERATION_MESSAGE, "createSnapshot"));
  }
}
