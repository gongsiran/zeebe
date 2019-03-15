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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.DefaultZeebeDbFactory;
import io.zeebe.db.impl.rocksdb.DbContext;
import io.zeebe.db.impl.rocksdb.DbContext.BufferSupplier;
import java.io.File;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.rocksdb.WriteOptions;

public class DbContextTest {
  private final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
      DefaultZeebeDbFactory.getDefaultFactory(DefaultColumnFamily.class);
  private ColumnFamily columnFamily;
  private DbContext dbContext;

  private final DbLong key = new DbLong();
  private final DbLong value = new DbLong();
  private ZeebeDb<DefaultColumnFamily> zeebeDb;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    final File pathName = tempFolder.newFolder();
    zeebeDb = Mockito.spy(((ZeebeTransactionDb<DefaultColumnFamily>) dbFactory.createDb(pathName)));

    dbContext = Mockito.spy(new DbContext());
    dbContext.setTransactionProvider(zeebeDb::getTransaction);

    columnFamily = zeebeDb.createColumnFamily(dbContext, DefaultColumnFamily.DEFAULT, key, value);
  }

  @Test
  public void shouldUseSameTransactionWhenNested() {
    dbContext.runInTransaction(
        () -> {
          final ZeebeTransaction exteriorTx = dbContext.getCurrentTransaction();
          dbContext.runInTransaction(
              () -> {
                final ZeebeTransaction interiorTx = dbContext.getCurrentTransaction();
                assertThat(exteriorTx).isEqualTo(interiorTx);
              });
        });

    Mockito.verify(zeebeDb, times(1)).getTransaction(any(WriteOptions.class));
  }

  @Test
  public void shouldReturnNullIfOverNestLimit() {
    try (BufferSupplier supplierOne = dbContext.getPrefixBufferSupplier()) {
      assertThat(supplierOne.getAvailablePrefixBuffer()).isNotNull();

      try (BufferSupplier supplierTwo = dbContext.getPrefixBufferSupplier()) {
        assertThat(supplierTwo.getAvailablePrefixBuffer()).isNotNull();

        try (BufferSupplier supplierThree = dbContext.getPrefixBufferSupplier()) {
          assertThat(supplierThree.getAvailablePrefixBuffer()).isNull();
        }
      }
    }
  }

  @Test
  public void shouldReleaseLastPrefixBuffer() {
    try (BufferSupplier supplierOne = dbContext.getPrefixBufferSupplier()) {
      assertThat(supplierOne.getAvailablePrefixBuffer()).isNotNull();

      try (BufferSupplier supplierTwo = dbContext.getPrefixBufferSupplier()) {
        assertThat(supplierTwo.getAvailablePrefixBuffer()).isNotNull();
      }
      try (BufferSupplier supplierThree = dbContext.getPrefixBufferSupplier()) {
        assertThat(supplierThree.getAvailablePrefixBuffer()).isNotNull();
      }
    }
  }
}
