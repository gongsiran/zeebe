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
package io.zeebe.broker.util;

import io.zeebe.broker.logstreams.state.DefaultZeebeDbFactory;
import io.zeebe.broker.logstreams.state.ZbColumnFamilies;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.rocksdb.DbContext;
import io.zeebe.protocol.Protocol;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class ZeebeStateRule extends ExternalResource {

  private final TemporaryFolder tempFolder = new TemporaryFolder();
  private ZeebeDb<ZbColumnFamilies> db;
  private ZeebeState zeebeState;
  private final int partition;

  public ZeebeStateRule() {
    this(Protocol.DEPLOYMENT_PARTITION);
  }

  public ZeebeStateRule(int partition) {
    this.partition = partition;
  }

  @Override
  protected void before() throws Throwable {
    tempFolder.create();
    db = createNewDb();
    final DbContext dbContext = new DbContext();
    dbContext.setTransactionProvider(db::getTransaction);

    zeebeState = new ZeebeState(partition, db, dbContext);
  }

  @Override
  protected void after() {
    try {
      db.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ZeebeState getZeebeState() {
    return zeebeState;
  }

  public ZeebeDb<ZbColumnFamilies> createNewDb() {
    try {
      final ZeebeDb<ZbColumnFamilies> db =
          DefaultZeebeDbFactory.DEFAULT_DB_FACTORY.createDb(tempFolder.newFolder());

      return db;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
