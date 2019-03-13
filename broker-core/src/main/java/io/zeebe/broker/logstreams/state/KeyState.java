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
package io.zeebe.broker.logstreams.state;

import io.zeebe.broker.logstreams.processor.KeyGenerator;
import io.zeebe.broker.workflow.state.NextValueManager;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.rocksdb.DbContext;
import io.zeebe.protocol.Protocol;

public class KeyState implements KeyGenerator {

  private static final long INITIAL_VALUE = 0;

  private static final String LATEST_KEY = "latestKey";

  private final long keyStartValue;
  private final NextValueManager nextValueManager;

  /**
   * Initializes the key state with the corresponding partition id, so that unique keys are
   * generated over all partitions.
   *
   * @param dbContext
   * @param partitionId the partition to determine the key start value
   */
  public KeyState(final DbContext dbContext, int partitionId, ZeebeDb zeebeDb) {
    keyStartValue = Protocol.encodePartitionId(partitionId, INITIAL_VALUE);
    nextValueManager = new NextValueManager(dbContext, keyStartValue, zeebeDb, ZbColumnFamilies.KEY);
  }

  @Override
  public long nextKey() {
    return nextValueManager.getNextValue(LATEST_KEY);
  }
}
