/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.transport.controlmessage;

import java.util.concurrent.CompletableFuture;

import org.agrona.DirectBuffer;
import io.zeebe.broker.logstreams.BrokerEventMetadata;
import io.zeebe.broker.task.TaskSubscriptionManager;
import io.zeebe.broker.task.processor.TaskSubscription;
import io.zeebe.broker.transport.clientapi.ErrorResponseWriter;
import io.zeebe.protocol.clientapi.ControlMessageType;
import io.zeebe.protocol.clientapi.ErrorCode;

public class AddTaskSubscriptionHandler implements ControlMessageHandler
{
    protected final TaskSubscription subscription = new TaskSubscription();

    protected final TaskSubscriptionManager manager;

    protected final ControlMessageResponseWriter responseWriter;
    protected final ErrorResponseWriter errorResponseWriter;

    public AddTaskSubscriptionHandler(TaskSubscriptionManager manager, ControlMessageResponseWriter responseWriter, ErrorResponseWriter errorResponseWriter)
    {
        this.manager = manager;
        this.responseWriter = responseWriter;
        this.errorResponseWriter = errorResponseWriter;
    }

    @Override
    public ControlMessageType getMessageType()
    {
        return ControlMessageType.ADD_TASK_SUBSCRIPTION;
    }

    @Override
    public CompletableFuture<Void> handle(DirectBuffer buffer, BrokerEventMetadata eventMetada)
    {
        subscription.reset();

        subscription.wrap(buffer);

        subscription.setChannelId(eventMetada.getReqChannelId());

        final CompletableFuture<Void> future = manager.addSubscription(subscription);

        return future.handle((v, failure) ->
        {
            if (failure == null)
            {
                responseWriter
                    .brokerEventMetadata(eventMetada)
                    .dataWriter(subscription)
                    .tryWriteResponse();
            }
            else
            {
                errorResponseWriter
                    .metadata(eventMetada)
                    .errorCode(ErrorCode.REQUEST_PROCESSING_FAILURE)
                    .errorMessage("Cannot add task subscription. %s", failure.getMessage())
                    .failedRequest(buffer, 0, buffer.capacity())
                    .tryWriteResponseOrLogFailure();
            }
            return null;
        });
    }

}