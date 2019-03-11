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
package io.zeebe.db.impl.rocksdb.readonly;

import io.zeebe.db.impl.DbLong;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.agrona.ExpandableArrayBuffer;

public class ZeebeDbReusableBuffers implements AutoCloseable {
  private ConcurrentLinkedQueue<ZeebeDbReusableBuffers> bufferPool;

  private DbLong key = new DbLong();
  private DbLong value = new DbLong();
  private ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();

  public ZeebeDbReusableBuffers(ConcurrentLinkedQueue pool) {
    bufferPool = pool;
  }

  public DbLong getKey() {
    return key;
  }

  public DbLong getValue() {
    return value;
  }

  public ExpandableArrayBuffer getKeyBuffer() {
    return keyBuffer;
  }

  public ExpandableArrayBuffer getValueBuffer() {
    return valueBuffer;
  }

  @Override
  public void close() {
    bufferPool.offer(this);
  }
}
