package io.zeebe.db.impl.rocksdb;

import io.zeebe.db.impl.rocksdb.transaction.ZeebeTransaction;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

// TODO: write class documentation
public class DbContext {
  private Function<WriteOptions, Transaction> transactionProvider;
  private final WriteOptions writeOptions;
  private ZeebeTransaction currentTransaction;

  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer prefixKeyBuffer = new ExpandableArrayBuffer();

  private final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);

  private boolean activePrefixIteration = false;

  public DbContext() {
    this(new WriteOptions());
  }

  public DbContext(final WriteOptions writeOptions) {
    this.writeOptions = writeOptions;
  }

  public ExpandableArrayBuffer getKeyBuffer() {
    return keyBuffer;
  }

  public ExpandableArrayBuffer getValueBuffer() {
    return valueBuffer;
  }

  public DirectBuffer getKeyViewBuffer() {
    return keyViewBuffer;
  }

  public DirectBuffer getValueViewBuffer() {
    return valueViewBuffer;
  }

  public ExpandableArrayBuffer getPrefixKeyBuffer() {
    return prefixKeyBuffer;
  }

  public ZeebeTransaction getTransaction() {
    if (currentTransaction == null) {
      currentTransaction = new ZeebeTransaction(transactionProvider.apply(writeOptions));
    }
    return currentTransaction;
  }

  public void setTransactionProvider(
      final Function<WriteOptions, Transaction> transactionDelegate) {
    this.transactionProvider = transactionDelegate;
  }

  public boolean isPrefixIterationActive() {
    return activePrefixIteration;
  }

  public void setPrefixIteration(final boolean status) {
    activePrefixIteration = status;
  }
}
