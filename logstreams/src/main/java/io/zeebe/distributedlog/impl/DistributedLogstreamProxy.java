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
package io.zeebe.distributedlog.impl;

import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.utils.concurrent.Futures;
import io.zeebe.distributedlog.AsyncDistributedLogstream;
import io.zeebe.distributedlog.CommitLogEvent;
import io.zeebe.distributedlog.DistributedLogstream;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.LogEventListener;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogstreamProxy
    extends AbstractAsyncPrimitive<AsyncDistributedLogstream, DistributedLogstreamService>
    implements AsyncDistributedLogstream, DistributedLogstreamClient {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedLogstreamProxy.class);
  private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(1000);

  private Set<LogEventListener> eventListeners;

  protected DistributedLogstreamProxy(
      ProxyClient<DistributedLogstreamService> client, PrimitiveRegistry registry) {
    super(client, registry);
    eventListeners = new LinkedHashSet<>();
  }

  @Override
  public CompletableFuture<Void> append(
      String partition, long commitPosition, byte[] buffer) {
    //final byte[] buffer = new byte[blockBuffer.remaining()];
   // blockBuffer.get(buffer);

    return getProxyClient().acceptBy(partition, service -> service.append(commitPosition, buffer));
  }

  @Override
  public DistributedLogstream sync() {
    return sync(DEFAULT_TIMEOUT);
  }

  @Override
  public DistributedLogstream sync(Duration duration) {
    return new BlockingDistributedLogstream(this, duration.toMillis());
  }

  @Override
  public void change(CommitLogEvent event) {
    eventListeners.forEach(listener -> listener.onCommit(event));
  }

  @Override
  public CompletableFuture<AsyncDistributedLogstream> connect() {
    return super.connect()
        .thenCompose(
            v ->
                Futures.allOf(
                    this.getProxyClient().getPartitions().stream().map(ProxySession::connect)))
        .thenApply(v -> this);
  }
}
