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
package io.zeebe.broker.workflow.processor.handlers.gateway;

import io.zeebe.broker.workflow.model.element.ExecutableExclusiveGateway;
import io.zeebe.broker.workflow.model.element.ExecutableSequenceFlow;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.handlers.IOMappingHelper;
import io.zeebe.broker.workflow.processor.handlers.element.ElementActivatingHandler;
import io.zeebe.msgpack.el.CompiledJsonCondition;
import io.zeebe.msgpack.el.JsonConditionException;
import io.zeebe.msgpack.el.JsonConditionInterpreter;
import io.zeebe.protocol.BpmnElementType;
import io.zeebe.protocol.impl.record.value.incident.ErrorType;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.util.List;
import org.agrona.DirectBuffer;

public class ExclusiveGatewayElementActivatingHandler<T extends ExecutableExclusiveGateway>
    extends ElementActivatingHandler<T> {
  private static final String NO_OUTGOING_FLOW_CHOSEN_ERROR =
      "Expected at least one condition to evaluate to true, or to have a default flow";
  private final WorkflowInstanceRecord record = new WorkflowInstanceRecord();
  private final JsonConditionInterpreter interpreter;

  public ExclusiveGatewayElementActivatingHandler() {
    this(new JsonConditionInterpreter());
  }

  public ExclusiveGatewayElementActivatingHandler(JsonConditionInterpreter interpreter) {
    super();
    this.interpreter = interpreter;
  }

  public ExclusiveGatewayElementActivatingHandler(
      WorkflowInstanceIntent nextState, JsonConditionInterpreter interpreter) {
    super(nextState);
    this.interpreter = interpreter;
  }

  public ExclusiveGatewayElementActivatingHandler(
      WorkflowInstanceIntent nextState,
      IOMappingHelper ioMappingHelper,
      JsonConditionInterpreter interpreter) {
    super(nextState, ioMappingHelper);
    this.interpreter = interpreter;
  }

  @Override
  protected boolean handleState(BpmnStepContext<T> context) {
    if (!super.handleState(context)) {
      return false;
    }

    final WorkflowInstanceRecord value = context.getValue();

    final DirectBuffer payload =
        context
            .getElementInstanceState()
            .getVariablesState()
            .getVariablesAsDocument(context.getRecord().getKey());
    final ExecutableSequenceFlow sequenceFlow;

    try {
      sequenceFlow = getSequenceFlowWithFulfilledCondition(context.getElement(), payload);
    } catch (JsonConditionException e) {
      context.raiseIncident(ErrorType.CONDITION_ERROR, e.getMessage());
      return false;
    }

    if (sequenceFlow == null) {
      context.raiseIncident(ErrorType.CONDITION_ERROR, NO_OUTGOING_FLOW_CHOSEN_ERROR);
      return false;
    }

    deferSequenceFlowTaken(context, value, sequenceFlow);
    return true;
  }

  private void deferSequenceFlowTaken(
      BpmnStepContext<T> context,
      WorkflowInstanceRecord value,
      ExecutableSequenceFlow sequenceFlow) {
    record.wrap(value);
    record.setElementId(sequenceFlow.getId());
    record.setBpmnElementType(BpmnElementType.SEQUENCE_FLOW);

    context
        .getOutput()
        .deferRecord(
            context.getRecord().getKey(), record, WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN);
  }

  private ExecutableSequenceFlow getSequenceFlowWithFulfilledCondition(
      ExecutableExclusiveGateway exclusiveGateway, DirectBuffer payload) {
    final List<ExecutableSequenceFlow> sequenceFlows = exclusiveGateway.getOutgoingWithCondition();

    for (final ExecutableSequenceFlow sequenceFlow : sequenceFlows) {
      final CompiledJsonCondition compiledCondition = sequenceFlow.getCondition();
      final boolean isFulFilled = interpreter.eval(compiledCondition.getCondition(), payload);

      if (isFulFilled) {
        return sequenceFlow;
      }
    }

    return exclusiveGateway.getDefaultFlow();
  }
}
