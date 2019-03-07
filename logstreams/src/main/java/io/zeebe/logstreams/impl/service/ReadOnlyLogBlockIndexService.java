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
package io.zeebe.logstreams.impl.service;

import static io.zeebe.logstreams.impl.LogBlockIndexWriter.LOG;

import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.impl.log.index.LogBlockColumnFamilies;
import io.zeebe.logstreams.impl.log.index.ReadOnlyLogBlockIndex;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import java.io.File;

public class ReadOnlyLogBlockIndexService implements Service<ReadOnlyLogBlockIndex> {

  private final File runtimeDirectory;
  private ReadOnlyLogBlockIndex logBlockIndex;

  public ReadOnlyLogBlockIndexService(File runtimeDirectory) {
    this.runtimeDirectory = runtimeDirectory;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    final ZeebeDbFactory<LogBlockColumnFamilies> dbFactory =
        ZeebeRocksDbFactory.newFactory(LogBlockColumnFamilies.class);

    logBlockIndex = new ReadOnlyLogBlockIndex(dbFactory, runtimeDirectory);
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    if (logBlockIndex != null) {
      try {
        logBlockIndex.closeDb();
      } catch (Exception e) {
        LOG.error("Couldn't close block index db", e);
      }
    }

    logBlockIndex = null;
  }

  @Override
  public ReadOnlyLogBlockIndex get() {
    return logBlockIndex;
  }
}
