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
package io.zeebe.distributedlog;

import static io.zeebe.distributedlog.DistributedLogRule.LOG;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.protocol.Protocol;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class DistributedLogTest {

  public ActorSchedulerRule actorSchedulerRule1 = new ActorSchedulerRule();
  public ActorSchedulerRule actorSchedulerRule2 = new ActorSchedulerRule();
  public ActorSchedulerRule actorSchedulerRule3 = new ActorSchedulerRule();

  public ServiceContainerRule serviceContainerRule1 = new ServiceContainerRule(actorSchedulerRule1);

  public ServiceContainerRule serviceContainerRule2 = new ServiceContainerRule(actorSchedulerRule2);

  public ServiceContainerRule serviceContainerRule3 = new ServiceContainerRule(actorSchedulerRule3);

  private static List<String> members = Arrays.asList("1", "2", "3");

  public DistributedLogRule node1 =
      new DistributedLogRule(
          serviceContainerRule1, 1, numPartitions, replicationFactor, members, null);

  public DistributedLogRule node2 =
      new DistributedLogRule(
          serviceContainerRule2, 2, 1, 3, members, Collections.singletonList(node1.getNode()));

  public DistributedLogRule node3 =
      new DistributedLogRule(
          serviceContainerRule3, 3, 1, 3, members, Collections.singletonList(node2.getNode()));

  public Timeout timeoutRule = Timeout.seconds(120);

  public static final int DEFAULT_RETRIES = 500;

  private static int partitionId = Protocol.START_PARTITION_ID;
  private static int numPartitions = 1;
  private static int replicationFactor = 3;

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(timeoutRule)
          .around(actorSchedulerRule1)
          .around(serviceContainerRule1)
          .around(actorSchedulerRule2)
          .around(serviceContainerRule2)
          .around(actorSchedulerRule3)
          .around(serviceContainerRule3)
          .around(node1)
          .around(node2)
          .around(node3);

  @Test
  public void shouldReplicateSingleEvent()
      throws ExecutionException, InterruptedException, TimeoutException {
    node1.waitUntilNodesJoined();
    node2.waitUntilNodesJoined();
    node3.waitUntilNodesJoined();

    // given
    node1.becomeLeader(partitionId);

    // when
    final Event event = writeEvent("record");

    // then
    assertEventReplicated(event);
  }

  @Test
  public void shouldReplicateMultipleEvents()
      throws ExecutionException, InterruptedException, TimeoutException {

    node1.waitUntilNodesJoined();
    node2.waitUntilNodesJoined();
    node3.waitUntilNodesJoined();

    // given
    node1.becomeLeader(partitionId);

    // when
    final Event event1 = writeEvent("record1");
    final Event event2 = writeEvent("record2");
    final Event event3 = writeEvent("record3");

    // then
    assertEventReplicated(event1);
    assertEventReplicated(event2);
    assertEventReplicated(event3);

    assertEventsCount(node1, 3);
    assertEventsCount(node2, 3);
    assertEventsCount(node3, 3);
  }

  @Test
  public void shouldRecoverFromFailure()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {

    node1.waitUntilNodesJoined();
    node2.waitUntilNodesJoined();
    node3.waitUntilNodesJoined();

    // given
    node1.becomeLeader(partitionId);

    final Event event1 = writeEvent("record1");
    assertEventReplicated(event1, node2);
    assertEventReplicated(event1, node1);

    // when

    node2.stopNode();

    // Write some events when node 2 is away
    final Event event2 = writeEvent("record2");
    final Event event3 = writeEvent("record3");

    assertEventReplicated(event2, node1);
    assertEventReplicated(event2, node3);
    assertEventReplicated(event3, node1);
    assertEventReplicated(event3, node3);

    node2.restartNode();
    node2.waitUntilNodesJoined();
    LOG.info("Node 2 restarted");

    final Event event4 = writeEvent("record4");

    // then

    // events are replicated in other nodes
    assertEventReplicated(event4, node1);
    assertEventReplicated(event4, node3);
    assertEventsCount(node1, 4);
    assertEventsCount(node3, 4);

    // node 2 can recover
    assertEventReplicated(event2, node2);
    assertEventReplicated(event3, node2);
    assertEventReplicated(event4, node2);

    // ensure no redundant writes
    assertEventsCount(node2, 4);
  }

  private Event writeEvent(String message) {
    final Event event = new Event();
    event.message = message;
    event.position = node1.writeEvent(partitionId, message);
    return event;
  }

  private class Event {
    String message;
    long position;
  }

  private void assertEventReplicated(Event event) {
    assertEventReplicated(event, node1);
    assertEventReplicated(event, node2);
    assertEventReplicated(event, node3);
  }

  private void assertEventReplicated(Event event, DistributedLogRule node) {
    TestUtil.waitUntil(
        () -> node.eventAppended(partitionId, event.message, event.position), DEFAULT_RETRIES);
  }

  private void assertEventsCount(DistributedLogRule node, int expectedCount) {
    assertThat(node.getCommittedEventsCount(partitionId)).isEqualTo(expectedCount);
  }
}
