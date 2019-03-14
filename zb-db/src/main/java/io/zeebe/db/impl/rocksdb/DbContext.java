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
package io.zeebe.db.impl.rocksdb;

import io.zeebe.db.TransactionOperation;
import io.zeebe.db.impl.rocksdb.transaction.ZeebeTransaction;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

// TODO: write class documentation
public class DbContext {

  private Function<WriteOptions, Transaction> transactionProvider;
  private WriteOptions writeOptions = null;
  private ZeebeTransaction currentZeebeTransaction;

  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer prefixKeyBuffer = new ExpandableArrayBuffer();

  private final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);

  private int activePrefixIterations = 0;
  private final ExpandableArrayBuffer[] prefixKeyBuffers =
      new ExpandableArrayBuffer[] {new ExpandableArrayBuffer(), new ExpandableArrayBuffer()};

  public DbContext() {
    // writeOptions = new WriteOptions(); UnsatisfiedLinkError (RocksDb hasn't been loaded probably)
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

  public ZeebeTransaction getCurrentTransaction() {
    if (currentZeebeTransaction == null) {
      currentZeebeTransaction = new ZeebeTransaction(transactionProvider.apply(getWriteOptions()));
    }
    return currentZeebeTransaction;
  }

  public void setTransactionProvider(
      final Function<WriteOptions, Transaction> transactionDelegate) {
    this.transactionProvider = transactionDelegate;
  }

  public void runInTransaction(TransactionOperation operations) {
    final Transaction rocksTransaction = transactionProvider.apply(getWriteOptions());
    try {
      currentZeebeTransaction = new ZeebeTransaction(rocksTransaction);
      operations.run();
      rocksTransaction.commit();
    } catch (Exception e) {
      try {
        rocksTransaction.rollback();
      } catch (RocksDBException e1) {
        throw new RuntimeException("Unexpected error occurred when rolling back transaction ", e);
      }
    }
  }

  private WriteOptions getWriteOptions() {
    if (writeOptions == null) {
      writeOptions = new WriteOptions();
    }

    return writeOptions;
  }

  public int getActivePrefixIterations() {
    return activePrefixIterations;
  }

  public void incrementActivePrefixIterations() {
    this.activePrefixIterations++;
  }

  public void decrementActivePrefixIterations() {
    this.activePrefixIterations--;
  }

  public ExpandableArrayBuffer[] getPrefixKeyBuffers() {
    return prefixKeyBuffers;
  }
}
