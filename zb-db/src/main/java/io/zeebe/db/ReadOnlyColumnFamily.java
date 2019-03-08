package io.zeebe.db;

import org.agrona.ExpandableArrayBuffer;

public interface ReadOnlyColumnFamily<KeyType extends DbKey, ValueType extends DbValue> {
  // TODO: rewrite documentation
  /**
   * The corresponding stored value in the column family to the given key.
   *
   * @param key the key
   * @return if the key was found in the column family then the value, otherwise null
   */
  ValueType get(
      KeyType key,
      ValueType value,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer writeBuffer);

  /**
   * Visits the key-value pairs, which are stored in the column family. The ordering depends on the
   * key. The visitor can indicate via the return value, whether the iteration should continue or
   * not. This means if the visitor returns false the iteration will stop.
   *
   * @param visitor the visitor which visits the key-value pairs
   * @param keyBuffer
   * @param valueBuffer
   */
  void whileTrue(
      KeyValuePairVisitor<KeyType, ValueType> visitor,
      KeyType key,
      ValueType value,
      ExpandableArrayBuffer keyBuffer,
      ExpandableArrayBuffer valueBuffer);

  /**
   * Checks if the column family has any entry.
   *
   * @return <code>true</code> if the column family has no entry
   */
  boolean isEmpty();

  /**
   * Checks for key existence in the column family.
   *
   * @param key the key to look for
   * @return true if the key exist in this column family, false otherwise
   */
  boolean exists(KeyType key);
}
