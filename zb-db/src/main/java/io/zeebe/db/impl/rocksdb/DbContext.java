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
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

/** This */
public class DbContext {
  public static final String TRANSACTION_ERROR =
      "Unexpected error occurred during RocksDB transaction.";

  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);
  private final BufferSupplier bufferSupplier = new BufferSupplier();

  private Function<WriteOptions, Transaction> transactionProvider;
  private ZeebeTransaction currentZeebeTransaction;

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

  public ZeebeTransaction getCurrentTransaction() {
    return currentZeebeTransaction;
  }

  public BufferSupplier getPrefixBufferSupplier() {
    return bufferSupplier;
  }

  public void setTransactionProvider(
      final Function<WriteOptions, Transaction> transactionDelegate) {
    this.transactionProvider = transactionDelegate;
  }

  public void runInTransaction(TransactionOperation operations) {
    try {
      if (currentZeebeTransaction != null) {
        operations.run();
      } else {
        runInNewTransaction(operations);
      }
    } catch (Exception e) {
      throw new RuntimeException(TRANSACTION_ERROR, e);
    }
  }

  private void runInNewTransaction(final TransactionOperation operations) throws Exception {
    try (final WriteOptions options = new WriteOptions()) {
      final ZeebeTransaction transaction = getTransaction(options);
      operations.run();
      transaction.commit();
    } finally {
      if (currentZeebeTransaction != null) {
        currentZeebeTransaction.close();
        currentZeebeTransaction = null;
      }
    }
  }

  private ZeebeTransaction getTransaction(WriteOptions options) {
    if (currentZeebeTransaction == null) {
      currentZeebeTransaction = new ZeebeTransaction(transactionProvider.apply(options));
    }

    return currentZeebeTransaction;
  }

  // TODO: probably over-engineering
  public class BufferSupplier implements AutoCloseable {
    private int activePrefixIterations = 0;
    private final ExpandableArrayBuffer[] prefixKeyBuffers =
        new ExpandableArrayBuffer[] {new ExpandableArrayBuffer(), new ExpandableArrayBuffer()};

    public ExpandableArrayBuffer getAvailablePrefixBuffer() {
      if (activePrefixIterations < prefixKeyBuffers.length) {
        return prefixKeyBuffers[activePrefixIterations++];
      }
      return null;
    }

    @Override
    public void close() {
      activePrefixIterations--;
    }
  }
}
