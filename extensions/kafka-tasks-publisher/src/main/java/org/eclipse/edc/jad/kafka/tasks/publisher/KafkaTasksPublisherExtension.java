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

import org.eclipse.edc.controlplane.tasks.TaskObservable;
import org.eclipse.edc.jad.kafka.KafkaClientFactory;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

import static org.eclipse.edc.jad.kafka.tasks.publisher.KafkaTasksPublisherExtension.NAME;

/**
 * Extension that publishes control-plane tasks (negotiations and transfers) to Kafka.
 * <p>
 * Replaces both {@code negotiation-tasks-publisher-nats} and {@code transfer-tasks-publisher-nats}
 * with a single extension that routes tasks to the appropriate topic.
 */
@Extension(NAME)
public class KafkaTasksPublisherExtension implements ServiceExtension {

    public static final String NAME = "Kafka Tasks Publisher Extension";

    @Setting(key = "edc.tasks.kafka.negotiation.topic", description = "Kafka topic for negotiation tasks", defaultValue = "edc-negotiation-tasks")
    private String negotiationTopic;

    @Setting(key = "edc.tasks.kafka.transfer.topic", description = "Kafka topic for transfer tasks", defaultValue = "edc-transfer-tasks")
    private String transferTopic;

    @Setting(key = "edc.tasks.kafka.topic.create", description = "Auto-create task topics on startup", defaultValue = "true")
    private boolean topicCreate;

    @Setting(key = "edc.tasks.kafka.topic.partitions", description = "Number of partitions for auto-created task topics", defaultValue = "3")
    private int topicPartitions;

    @Inject
    private TaskObservable taskObservable;

    @Inject
    private KafkaClientFactory kafkaClientFactory;

    @Inject
    private TypeManager typeManager;

    private KafkaTaskPublisher publisher;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        if (topicCreate) {
            kafkaClientFactory.ensureTopicExists(negotiationTopic, topicPartitions, (short) 1);
            kafkaClientFactory.ensureTopicExists(transferTopic, topicPartitions, (short) 1);
        }

        var kafkaProducer = kafkaClientFactory.createProducer();
        var objectMapper = typeManager.getMapper();

        publisher = new KafkaTaskPublisher(kafkaProducer, negotiationTopic, transferTopic, objectMapper, context.getMonitor());

        // Register as a TaskListener — will be notified of all new tasks
        taskObservable.registerListener(publisher);

        context.getMonitor().info("Kafka Tasks Publisher initialized — topics: "
                + negotiationTopic + ", " + transferTopic);
    }

    @Override
    public void shutdown() {
        if (publisher != null) {
            taskObservable.unregisterListener(publisher);
            publisher.close();
        }
    }
}
