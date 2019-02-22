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
package io.zeebe.broker.workflow.processor.handlers.seqflow;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.model.element.ExecutableSequenceFlow;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.EventOutput;
import io.zeebe.broker.workflow.processor.handlers.AbstractHandler;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.IndexedRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.ArrayList;
import java.util.List;

public class ParallelMergeSequenceFlowTaken<T extends ExecutableSequenceFlow>
    extends AbstractHandler<T> {
  public ParallelMergeSequenceFlowTaken() {
    super(null);
  }

  public ParallelMergeSequenceFlowTaken(WorkflowInstanceIntent nextState) {
    super(nextState);
  }

  @Override
  protected boolean shouldHandleState(BpmnStepContext<T> context) {
    return super.shouldHandleState(context) && isElementActive(context.getFlowScopeInstance());
  }

  @Override
  protected boolean handleState(BpmnStepContext<T> context) {
    final ElementInstance scopeInstance = context.getFlowScopeInstance();
    final EventOutput eventOutput = context.getOutput();
    final ExecutableSequenceFlow sequenceFlow = context.getElement();
    final ExecutableFlowNode gateway = sequenceFlow.getTarget();

    final List<IndexedRecord> mergeableRecords =
        getMergeableRecords(context, gateway, scopeInstance);

    final TypedRecord<WorkflowInstanceRecord> record = context.getRecord();
    final WorkflowInstanceRecord value = record.getValue();
    final WorkflowInstanceIntent intent = (WorkflowInstanceIntent) record.getMetadata().getIntent();
    mergeableRecords.add(new IndexedRecord(record.getKey(), intent, value));
    if (mergeableRecords.size() == gateway.getIncoming().size()) {

      // consume all deferred tokens and spawn a new one to continue after the gateway
      mergeableRecords.forEach(
          r -> {
            eventOutput.removeDeferredEvent(scopeInstance.getKey(), r.getKey());
            context.getFlowScopeInstance().consumeToken();
          });

      context
          .getOutput()
          .appendNewEvent(WorkflowInstanceIntent.ELEMENT_ACTIVATING, context.getValue(), gateway);
      context.getFlowScopeInstance().spawnToken();
    }

    return true;
  }

  /** @return the records that can be merged */
  private List<IndexedRecord> getMergeableRecords(
      BpmnStepContext<T> context,
      ExecutableFlowNode parallelGateway,
      ElementInstance scopeInstance) {
    final List<ExecutableSequenceFlow> incomingFlows = parallelGateway.getIncoming();
    final List<IndexedRecord> mergingRecords = new ArrayList<>(incomingFlows.size());

    final List<IndexedRecord> storedRecords =
        context.getElementInstanceState().getDeferredRecords(scopeInstance.getKey());

    for (final ExecutableSequenceFlow flow : incomingFlows) {
      for (final IndexedRecord recordToMatch : storedRecords) {
        if (recordToMatch.getValue().getElementId().equals(flow.getId())) {
          mergingRecords.add(recordToMatch);
          break;
        }
      }
    }

    return mergingRecords;
  }
}
