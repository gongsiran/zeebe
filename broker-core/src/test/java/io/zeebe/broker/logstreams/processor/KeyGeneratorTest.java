/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams.processor;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.logstreams.state.ZbColumnFamilies;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.util.ZeebeStateRule;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.rocksdb.DbContext;
import io.zeebe.protocol.Protocol;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class KeyGeneratorTest {

  @Rule public ZeebeStateRule stateRule = new ZeebeStateRule();

  private KeyGenerator keyGenerator;

  @Before
  public void setUp() throws Exception {
    keyGenerator = stateRule.getZeebeState().getKeyGenerator();
  }

  @Test
  public void shouldGetFirstValue() {
    // given

    // when
    final long firstKey = keyGenerator.nextKey();

    // then
    assertThat(firstKey).isEqualTo(1);
  }

  @Test
  public void shouldGetNextValue() {
    // given
    final long key = keyGenerator.nextKey();

    // when
    final long nextKey = keyGenerator.nextKey();

    // then
    assertThat(nextKey).isGreaterThan(key);
  }

  @Test
  public void shouldGetUniqueValuesOverPartitions() throws Exception {
    // given
    final ZeebeDb<ZbColumnFamilies> newDb = stateRule.createNewDb();
    final DbContext dbContext = new DbContext();
    dbContext.setTransactionProvider(newDb::getTransaction);

    final ZeebeState otherZeebeState = new ZeebeState(1, newDb, dbContext);
    final KeyGenerator keyGenerator2 = otherZeebeState.getKeyGenerator();

    final long keyOfFirstPartition = keyGenerator.nextKey();

    // when
    final long keyOfSecondPartition = keyGenerator2.nextKey();

    // then
    assertThat(keyOfFirstPartition).isNotEqualTo(keyOfSecondPartition);

    assertThat(Protocol.decodePartitionId(keyOfFirstPartition)).isEqualTo(0);
    assertThat(Protocol.decodePartitionId(keyOfSecondPartition)).isEqualTo(1);

    newDb.close();
  }
}
