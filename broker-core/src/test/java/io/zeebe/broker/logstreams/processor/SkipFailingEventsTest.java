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

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.broker.logstreams.state.DefaultZeebeDbFactory;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.util.MockTypedRecord;
import io.zeebe.broker.util.Records;
import io.zeebe.broker.util.StreamProcessorControl;
import io.zeebe.broker.util.TestStreams;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.transport.ServerOutput;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SkipFailingEventsTest {
  public static final String STREAM_NAME = "foo";
  public static final int STREAM_PROCESSOR_ID = 144144;

  public TemporaryFolder tempFolder = new TemporaryFolder();
  public AutoCloseableRule closeables = new AutoCloseableRule();

  public ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule();
  public ServiceContainerRule serviceContainerRule = new ServiceContainerRule(actorSchedulerRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(tempFolder)
          .around(actorSchedulerRule)
          .around(serviceContainerRule)
          .around(closeables);

  protected TestStreams streams;
  protected LogStream stream;

  @Mock protected ServerOutput output;
  private StreamProcessorControl streamProcessorControl;
  private KeyGenerator keyGenerator;
  private TypedStreamEnvironment env;
  private ZeebeState zeebeState;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    streams =
        new TestStreams(
            tempFolder.getRoot(), closeables, serviceContainerRule.get(), actorSchedulerRule.get());

    stream = streams.createLogStream(STREAM_NAME);
    env = new TypedStreamEnvironment(streams.getLogStream(STREAM_NAME), output);

    final AtomicLong key = new AtomicLong();
    keyGenerator = () -> key.getAndIncrement();
  }

  @Test
  public void shouldSkipFailingEvent() {
    // given
    final DumpProcessor dumpProcessor = spy(new DumpProcessor());
    final ErrorProneProcessor processor = new ErrorProneProcessor();

    streamProcessorControl =
        streams.initStreamProcessor(
            STREAM_NAME,
            STREAM_PROCESSOR_ID,
            DefaultZeebeDbFactory.DEFAULT_DB_FACTORY,
            (db) -> {
              zeebeState = new ZeebeState(db);
              return env.newStreamProcessor()
                  .zeebeState(zeebeState)
                  .onEvent(
                      ValueType.WORKFLOW_INSTANCE,
                      WorkflowInstanceIntent.ELEMENT_ACTIVATING,
                      processor)
                  .onEvent(
                      ValueType.WORKFLOW_INSTANCE,
                      WorkflowInstanceIntent.ELEMENT_ACTIVATED,
                      dumpProcessor)
                  .build();
            });
    streamProcessorControl.start();

    streams
        .newRecord(STREAM_NAME)
        .event(workflowInstance(1))
        .recordType(RecordType.EVENT)
        .intent(WorkflowInstanceIntent.ELEMENT_ACTIVATING)
        .key(keyGenerator.nextKey())
        .write();
    streams
        .newRecord(STREAM_NAME)
        .event(workflowInstance(1))
        .recordType(RecordType.EVENT)
        .intent(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
        .key(keyGenerator.nextKey())
        .write();

    // other instance
    streams
        .newRecord(STREAM_NAME)
        .event(workflowInstance(2))
        .recordType(RecordType.EVENT)
        .intent(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
        .key(keyGenerator.nextKey())
        .write();
    // when
    streamProcessorControl.unblock();

    doRepeatedly(
            () ->
                streams
                    .events(STREAM_NAME)
                    .filter(
                        e ->
                            Records.isEvent(
                                e,
                                ValueType.WORKFLOW_INSTANCE,
                                WorkflowInstanceIntent.ELEMENT_COMPLETED))
                    .findFirst())
        .until(o -> o.isPresent())
        .get();

    // then
    assertThat(processor.getProcessCount()).isEqualTo(1);

    final RecordMetadata metadata = new RecordMetadata();
    metadata.valueType(ValueType.WORKFLOW_INSTANCE);
    final MockTypedRecord<WorkflowInstanceRecord> mockTypedRecord =
        new MockTypedRecord<>(0, metadata, workflowInstance(1));
    assertThat(zeebeState.isOnBlacklist(mockTypedRecord)).isTrue();

    verify(dumpProcessor, times(1)).processRecord(any(), any(), any(), any());
    assertThat(dumpProcessor.processedInstances).containsExactly(2L);
  }

  protected WorkflowInstanceRecord workflowInstance(final int instanceKey) {
    final WorkflowInstanceRecord event = new WorkflowInstanceRecord();
    event.setWorkflowInstanceKey(instanceKey);
    return event;
  }

  protected static class ErrorProneProcessor
      implements TypedRecordProcessor<WorkflowInstanceRecord> {

    public final AtomicLong processCount = new AtomicLong(0);

    @Override
    public void processRecord(
        final TypedRecord<WorkflowInstanceRecord> record,
        final TypedResponseWriter responseWriter,
        final TypedStreamWriter streamWriter) {
      processCount.incrementAndGet();
      throw new RuntimeException("expected");
    }

    public long getProcessCount() {
      return processCount.get();
    }
  }

  protected static class DumpProcessor implements TypedRecordProcessor<WorkflowInstanceRecord> {
    final List<Long> processedInstances = new ArrayList<>();

    @Override
    public void processRecord(
        TypedRecord<WorkflowInstanceRecord> record,
        TypedResponseWriter responseWriter,
        TypedStreamWriter streamWriter) {
      processedInstances.add(record.getValue().getWorkflowInstanceKey());
      streamWriter.appendFollowUpEvent(
          record.getKey(), WorkflowInstanceIntent.ELEMENT_COMPLETED, record.getValue());
    }
  }
}
