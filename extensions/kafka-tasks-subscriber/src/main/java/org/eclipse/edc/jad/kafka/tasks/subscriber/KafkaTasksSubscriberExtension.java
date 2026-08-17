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

import org.eclipse.edc.controlplane.tasks.TaskService;
import org.eclipse.edc.jad.kafka.KafkaClientFactory;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

import java.util.List;

import static org.eclipse.edc.jad.kafka.tasks.subscriber.KafkaTasksSubscriberExtension.NAME;

/**
 * Extension that subscribes to Kafka task topics and dispatches tasks to the local control plane.
 * <p>
 * Replaces both {@code negotiation-tasks-subscriber-nats} and {@code transfer-tasks-subscriber-nats}
 * with a single extension that consumes from both task topics.
 * <p>
 * Uses Kafka consumer groups for work distribution: multiple control plane replicas in the same
 * group will have tasks distributed among them (each task processed by exactly one replica).
 */
@Extension(NAME)
public class KafkaTasksSubscriberExtension implements ServiceExtension {

    public static final String NAME = "Kafka Tasks Subscriber Extension";

    @Setting(key = "edc.tasks.kafka.negotiation.topic", description = "Kafka topic for negotiation tasks", defaultValue = "edc-negotiation-tasks")
    private String negotiationTopic;

    @Setting(key = "edc.tasks.kafka.transfer.topic", description = "Kafka topic for transfer tasks", defaultValue = "edc-transfer-tasks")
    private String transferTopic;

    @Setting(key = "edc.tasks.kafka.consumer.group", description = "Kafka consumer group for task distribution", defaultValue = "edc-tasks-group")
    private String consumerGroup;

    @Setting(key = "edc.tasks.kafka.consumer.auto.offset.reset", description = "Auto offset reset (earliest/latest)", defaultValue = "earliest")
    private String autoOffsetReset;

    @Inject
    private TaskService taskService;

    @Inject
    private KafkaClientFactory kafkaClientFactory;

    @Inject
    private TypeManager typeManager;

    private KafkaTaskSubscriber subscriber;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var kafkaConsumer = kafkaClientFactory.createConsumer(consumerGroup, autoOffsetReset);
        var objectMapper = typeManager.getMapper();

        var topics = List.of(negotiationTopic, transferTopic);

        subscriber = new KafkaTaskSubscriber(kafkaConsumer, topics, taskService, objectMapper, context.getMonitor());

        context.getMonitor().info("Kafka Tasks Subscriber initialized — group: " + consumerGroup
                + ", topics: " + topics);
    }

    @Override
    public void start() {
        if (subscriber != null) {
            subscriber.start();
        }
    }

    @Override
    public void shutdown() {
        if (subscriber != null) {
            subscriber.stop();
        }
    }
}
