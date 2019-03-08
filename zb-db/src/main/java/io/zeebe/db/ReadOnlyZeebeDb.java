package io.zeebe.db;

public interface ReadOnlyZeebeDb<ColumnFamilyNames extends Enum<ColumnFamilyNames>>
    extends AutoCloseable {
  /**
   * Creates an instance of a specific column family to access and store key-value pairs in that
   * column family. The key and value instances are used to ensure type safety.
   *
   * <p>If the column family instance is created only the defined key and value types can be stored
   * in the column family.
   *
   * @param columnFamily the enum instance of the column family
   * @param keyInstance this instance defines the type of the column family key type
   * @param valueInstance this instance defines the type of the column family value type
   * @param <KeyType> the key type of the column family
   * @param <ValueType> the value type of the column family
   * @return the created column family instance
   */
  <KeyType extends DbKey, ValueType extends DbValue>
      ReadOnlyColumnFamily<KeyType, ValueType> createColumnFamily(
          ColumnFamilyNames columnFamily, KeyType keyInstance, ValueType valueInstance);
}
