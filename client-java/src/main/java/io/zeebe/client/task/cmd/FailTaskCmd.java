package io.zeebe.client.task.cmd;

import java.util.Map;

import io.zeebe.client.cmd.SetPayloadCmd;

public interface FailTaskCmd extends SetPayloadCmd<Long, FailTaskCmd>
{
    /**
     * Set the key of the task.
     */
    FailTaskCmd taskKey(long taskKey);

    /**
     * Set the owner who worked on the task.
     */
    FailTaskCmd lockOwner(String lockOwner);

    /**
     * Set the type of the task.
     */
    FailTaskCmd taskType(String taskType);

    /**
     * Add the given key-value-pair to the task header.
     */
    FailTaskCmd addHeader(String key, Object value);

    /**
     * Sets the given key-value-pairs as the task header.
     */
    FailTaskCmd headers(Map<String, Object> headers);

    /**
     * Sets the error which causes the failure.
     */
    FailTaskCmd failure(Exception e);

    /**
     * Sets the remaining retries of the task. If the retries are equal to zero
     * then the task will not be locked again unless the retries are increased.
     */
    FailTaskCmd retries(int remainingRetries);

}