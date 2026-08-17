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

package org.eclipse.edc.jad.kafka.tasks.subscriber;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.edc.controlplane.tasks.Task;
import org.eclipse.edc.controlplane.tasks.TaskService;
import org.eclipse.edc.spi.monitor.Monitor;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Subscribes to Kafka task topics and dispatches received tasks to the {@link TaskService}
 * for execution by the local control plane.
 * <p>
 * Runs a dedicated consumer thread per subscribed topic. Uses manual offset commits
 * to ensure at-least-once delivery (offset committed only after successful task creation).
 * <p>
 * The EDC state machine is inherently idempotent (DB lease prevents double-processing),
 * so at-least-once is safe here — duplicate deliveries are naturally deduplicated by
 * the lease mechanism.
 */
public class KafkaTaskSubscriber {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final TaskService taskService;
    private final ObjectMapper objectMapper;
    private final Monitor monitor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor;

    public KafkaTaskSubscriber(KafkaConsumer<String, String> consumer,
                               List<String> topics,
                               TaskService taskService,
                               ObjectMapper objectMapper,
                               Monitor monitor) {
        this.consumer = consumer;
        this.topics = topics;
        this.taskService = taskService;
        this.objectMapper = objectMapper;
        this.monitor = monitor;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            var thread = new Thread(r, "kafka-task-subscriber");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Starts the consumer loop in a background thread.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            executor.submit(this::consumeLoop);
            monitor.info("Kafka Task Subscriber started — topics: " + topics);
        }
    }

    /**
     * Stops the consumer loop gracefully.
     */
    public void stop() {
        running.set(false);
        consumer.wakeup();
        executor.shutdown();
        monitor.info("Kafka Task Subscriber stopped");
    }

    private void consumeLoop() {
        try {
            consumer.subscribe(topics);

            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(500));

                for (var record : records) {
                    try {
                        var task = objectMapper.readValue(record.value(), Task.class);

                        monitor.debug("Received task '" + task.getName() + "' [id=" + task.getId()
                                + "] from topic " + record.topic()
                                + " [partition=" + record.partition() + ", offset=" + record.offset() + "]");

                        // Dispatch to the TaskService for execution
                        var result = taskService.create(task);
                        if (result.failed()) {
                            monitor.warning("Failed to create task from Kafka message: " + result.getFailureDetail()
                                    + " [topic=" + record.topic() + ", offset=" + record.offset() + "]");
                        }

                    } catch (Exception e) {
                        monitor.severe("Error processing task from Kafka [topic=" + record.topic()
                                + ", partition=" + record.partition()
                                + ", offset=" + record.offset() + "]", e);
                    }
                }

                // Commit offsets after processing the batch
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // Expected on shutdown
            if (running.get()) {
                monitor.warning("Unexpected wakeup in Kafka task consumer", e);
            }
        } catch (Exception e) {
            monitor.severe("Fatal error in Kafka task consumer loop", e);
        } finally {
            consumer.close();
        }
    }
}
