package io.zeebe.db.impl.rocksdb.readonly;

import io.zeebe.db.impl.DbLong;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.agrona.ExpandableArrayBuffer;

public class ReadOnlyRequestBuffers implements AutoCloseable {
  private ConcurrentLinkedQueue<ReadOnlyRequestBuffers> bufferPool;

  public DbLong key;
  public DbLong value;
  public ExpandableArrayBuffer keyBuffer;
  public ExpandableArrayBuffer valueBuffer;

  public ReadOnlyRequestBuffers(ConcurrentLinkedQueue pool) {
    bufferPool = pool;
    key = new DbLong();
    value = new DbLong();
    keyBuffer = new ExpandableArrayBuffer();
    valueBuffer = new ExpandableArrayBuffer();
  }

  @Override
  public void close() {
    bufferPool.offer(this);
  }
}
