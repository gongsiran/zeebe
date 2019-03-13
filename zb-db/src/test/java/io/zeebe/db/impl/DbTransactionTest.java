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
package io.zeebe.db.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.rocksdb.DbContext;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DbTransactionTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final ZeebeDbFactory<ColumnFamilies> dbFactory =
      DefaultZeebeDbFactory.getDefaultFactory(ColumnFamilies.class);

  private ZeebeDb<ColumnFamilies> zeebeDb;

  private ColumnFamily<DbLong, DbLong> oneColumnFamily;
  private ColumnFamily<DbLong, DbLong> twoColumnFamily;
  private ColumnFamily<DbLong, DbLong> threeColumnFamily;

  private DbLong oneKey;
  private DbLong oneValue;
  private DbLong twoValue;
  private DbLong twoKey;
  private DbLong threeKey;
  private DbLong threeValue;

  private enum ColumnFamilies {
    DEFAULT, // rocksDB needs a default column family
    ONE,
    TWO,
    THREE
  }

  @Before
  public void setup() throws Exception {
    final File pathName = temporaryFolder.newFolder();
    zeebeDb = dbFactory.createDb(pathName);
    final DbContext dbContext = new DbContext();
    dbContext.setTransactionProvider(zeebeDb::getTransaction);

    oneKey = new DbLong();
    oneValue = new DbLong();
    oneColumnFamily = zeebeDb.createColumnFamily(dbContext, ColumnFamilies.ONE, oneKey, oneValue);

    twoKey = new DbLong();
    twoValue = new DbLong();
    twoColumnFamily = zeebeDb.createColumnFamily(dbContext, ColumnFamilies.TWO, twoKey, twoValue);

    threeKey = new DbLong();
    threeValue = new DbLong();
    threeColumnFamily =
        zeebeDb.createColumnFamily(dbContext, ColumnFamilies.THREE, threeKey, threeValue);
  }

  @Test
  public void shouldUseTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    // when
    zeebeDb.transaction(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          twoColumnFamily.put(twoKey, twoValue);
          threeColumnFamily.put(threeKey, threeValue);
        });

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isTrue();
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    assertThat(threeColumnFamily.exists(threeKey)).isTrue();
  }

  @Test
  public void shouldGetValueInTransaction() {
    // given
    final AtomicLong actualValue = new AtomicLong(0);
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    // when
    zeebeDb.transaction(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          final DbLong value = oneColumnFamily.get(oneKey);
          actualValue.set(value.getValue());
        });

    // then
    assertThat(actualValue.get()).isEqualTo(-1);
    assertThat(oneColumnFamily.get(oneKey).getValue()).isEqualTo(-1);
  }

  @Test
  public void shouldFindValueInTransaction() {
    // given
    final Map<Long, Long> actualValues = new HashMap<>();
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    // when
    zeebeDb.transaction(
        () -> {
          // update value
          oneKey.wrapLong(1);
          oneValue.wrapLong(-2);
          oneColumnFamily.put(oneKey, oneValue);

          // create new key-value pair
          oneKey.wrapLong(2);
          oneValue.wrapLong(-3);
          oneColumnFamily.put(oneKey, oneValue);

          actualValues.put(oneKey.getValue(), oneColumnFamily.get(oneKey).getValue());
          oneKey.wrapLong(1);
          actualValues.put(oneKey.getValue(), oneColumnFamily.get(oneKey).getValue());
        });

    // then
    final Map<Long, Long> expectedValues = new HashMap<>();
    expectedValues.put(1L, -2L);
    expectedValues.put(2L, -3L);
    assertThat(actualValues).isEqualTo(expectedValues);
  }

  @Test
  public void shouldIterateAndFindValuesInTransaction() {
    // given
    final Map<Long, Long> actualValues = new HashMap<>();

    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    oneKey.wrapLong(2);
    oneValue.wrapLong(-2);
    oneColumnFamily.put(oneKey, oneValue);

    // when
    zeebeDb.transaction(
        () -> {
          // update old value
          oneKey.wrapLong(2);
          oneValue.wrapLong(-5);
          oneColumnFamily.put(oneKey, oneValue);

          // create new key-value pair
          oneKey.wrapLong(3);
          oneValue.wrapLong(-3);
          oneColumnFamily.put(oneKey, oneValue);

          oneColumnFamily.forEach((k, v) -> actualValues.put(k.getValue(), v.getValue()));
        });

    // then
    final Map<Long, Long> expectedValues = new HashMap<>();
    expectedValues.put(1L, -1L);
    expectedValues.put(2L, -5L);
    expectedValues.put(3L, -3L);
    assertThat(actualValues).isEqualTo(expectedValues);
  }

  @Test
  public void shouldIterateAndDeleteInTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    oneKey.wrapLong(2);
    oneValue.wrapLong(-2);
    oneColumnFamily.put(oneKey, oneValue);

    // when
    zeebeDb.transaction(() -> oneColumnFamily.forEach((k, v) -> oneColumnFamily.delete(k)));

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
    oneKey.wrapLong(2);
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
  }

  @Test
  public void shouldEndInSameTransaction() {
    // given
    final AtomicLong actualValue = new AtomicLong(0);
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    twoValue.wrapLong(192313);

    // when
    oneColumnFamily.put(oneKey, oneValue);
    zeebeDb.transaction(
        () -> {
          zeebeDb.transaction(() -> oneColumnFamily.put(oneKey, twoValue));
          final DbLong value = oneColumnFamily.get(oneKey);
          actualValue.set(value.getValue());
        });

    // then
    assertThat(actualValue.get()).isEqualTo(192313);
    assertThat(oneColumnFamily.get(oneKey).getValue()).isEqualTo(192313);
  }

  @Test
  public void shouldWriteAndDeleteInTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);
    twoColumnFamily.put(twoKey, twoValue);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);
    threeColumnFamily.put(threeKey, threeValue);

    // when
    zeebeDb.transaction(
        () -> {
          // create
          oneColumnFamily.put(oneKey, oneValue);

          // delete
          twoColumnFamily.delete(twoKey);

          // update
          threeValue.wrapLong(Integer.MIN_VALUE);
          threeColumnFamily.put(threeKey, threeValue);
        });

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isTrue();
    assertThat(oneColumnFamily.get(oneKey).getValue()).isEqualTo(-1);

    assertThat(twoColumnFamily.exists(twoKey)).isFalse();

    assertThat(threeColumnFamily.exists(threeKey)).isTrue();
    assertThat(threeColumnFamily.get(threeKey).getValue()).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void shouldWriteAndDeleteSameKeyValuePairInTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    // when
    zeebeDb.transaction(
        () -> {
          // create
          oneColumnFamily.put(oneKey, oneValue);

          // delete
          oneColumnFamily.delete(oneKey);
        });

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
  }

  @Test
  public void shouldNotCommitOnError() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);
    twoColumnFamily.put(twoKey, twoValue);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    // when
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    try {
      zeebeDb.transaction(
          () -> {
            oneColumnFamily.put(oneKey, oneValue);
            twoColumnFamily.delete(twoKey);
            threeColumnFamily.put(threeKey, threeValue);
            throw new RuntimeException();
          });
    } catch (Exception e) {
      // ignore
    }

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    assertThat(threeColumnFamily.exists(threeKey)).isFalse();
  }
}
