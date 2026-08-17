/*
 *  Copyright (c) 2025 Metaform Systems, Inc.
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Metaform Systems, Inc. - initial API and implementation
 *
 */

package org.eclipse.edc.jad.kafka.tasks.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.edc.controlplane.tasks.ProcessTaskPayload;
import org.eclipse.edc.controlplane.tasks.Task;
import org.eclipse.edc.controlplane.tasks.TaskListener;
import org.eclipse.edc.spi.monitor.Monitor;

import java.nio.charset.StandardCharsets;

/**
 * Publishes EDC control-plane tasks to Kafka topics.
 * <p>
 * Routes tasks to the appropriate topic based on the {@link ProcessTaskPayload#getProcessType()}:
 * <ul>
 *   <li>{@code negotiations} → negotiation tasks topic</li>
 *   <li>{@code transfers} → transfer tasks topic</li>
 * </ul>
 * <p>
 * Uses the task ID as the Kafka record key to ensure ordering per entity.
 * Idempotent delivery is guaranteed via the producer's {@code enable.idempotence=true} setting.
 */
public class KafkaTaskPublisher implements TaskListener {

    private final KafkaProducer<String, String> producer;
    private final String negotiationTopic;
    private final String transferTopic;
    private final ObjectMapper objectMapper;
    private final Monitor monitor;

    public KafkaTaskPublisher(KafkaProducer<String, String> producer,
                              String negotiationTopic,
                              String transferTopic,
                              ObjectMapper objectMapper,
                              Monitor monitor) {
        this.producer = producer;
        this.negotiationTopic = negotiationTopic;
        this.transferTopic = transferTopic;
        this.objectMapper = objectMapper;
        this.monitor = monitor;
    }

    @Override
    public void created(Task task) {
        try {
            var topic = resolveTopicForTask(task);
            var serialized = objectMapper.writeValueAsString(task);

            // Use task ID as key for partition affinity (all state transitions for
            // the same negotiation/transfer go to the same partition → ordering guarantee)
            var record = new ProducerRecord<>(topic, task.getId(), serialized);

            // Add metadata headers
            record.headers().add(new RecordHeader("task_name", task.getName().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("task_group", task.getGroup().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("retry_count",
                    String.valueOf(task.getRetryCount()).getBytes(StandardCharsets.UTF_8)));

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    monitor.warning("Failed to publish task '" + task.getId() + "' to Kafka topic '"
                            + topic + "': " + exception.getMessage());
                } else {
                    monitor.debug("Published task '" + task.getName() + "' [id=" + task.getId()
                            + "] to " + topic + " [partition=" + metadata.partition()
                            + ", offset=" + metadata.offset() + "]");
                }
            });

        } catch (JsonProcessingException e) {
            monitor.severe("Failed to serialize task '" + task.getId() + "' for Kafka publishing", e);
        }
    }

    /**
     * Resolves the target Kafka topic based on the task's process type.
     */
    private String resolveTopicForTask(Task task) {
        if (task.getPayload() instanceof ProcessTaskPayload processPayload) {
            var processType = processPayload.getProcessType();
            if ("negotiations".equals(processType)) {
                return negotiationTopic;
            } else if ("transfers".equals(processType)) {
                return transferTopic;
            }
        }
        // Default to negotiation topic for unrecognized task types
        monitor.warning("Unknown task process type for task '" + task.getName()
                + "', routing to negotiation topic as default");
        return negotiationTopic;
    }

    /**
     * Flushes pending messages and closes the producer.
     */
    public void close() {
        producer.flush();
        producer.close();
    }
}
