package io.zeebe.db;

public interface ReadOnlyColumnFamily<KeyType extends DbKey, ValueType extends DbValue> {
  // TODO: rewrite documentation
  /**
   * The corresponding stored value in the column family to the given key.
   *
   * @param key the key
   * @return if the key was found in the column family then the value, otherwise null
   */
  ValueType get(KeyType key);

  /**
   * Visits the key-value pairs, which are stored in the column family. The ordering depends on the
   * key. The visitor can indicate via the return value, whether the iteration should continue or
   * not. This means if the visitor returns false the iteration will stop.
   *  @param visitor the visitor which visits the key-value pairs
   *
   */
  void whileTrue(
    KeyValuePairVisitor<KeyType, ValueType> visitor);

}
