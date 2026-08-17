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

package org.eclipse.edc.jad.kafka.events;

import org.eclipse.edc.jad.kafka.KafkaClientFactory;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.event.Event;
import org.eclipse.edc.spi.event.EventRouter;
import org.eclipse.edc.spi.system.Hostname;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

import static org.eclipse.edc.jad.kafka.events.KafkaEventPublishingExtension.NAME;

/**
 * Extension that publishes all EDC domain events to a Kafka topic as CloudEvents.
 * <p>
 * This is a drop-in replacement for the upstream {@code events-nats} extension.
 * Configuration is via {@code edc.events.kafka.*} properties.
 */
@Extension(NAME)
public class KafkaEventPublishingExtension implements ServiceExtension {

    public static final String NAME = "Kafka Event Publishing Extension";

    @Setting(key = "edc.events.kafka.topic", description = "Kafka topic for EDC domain events", defaultValue = "edc-events")
    private String topic;

    @Setting(key = "edc.events.kafka.topic.create", description = "Auto-create the events topic on startup", defaultValue = "true")
    private boolean topicCreate;

    @Setting(key = "edc.events.kafka.topic.partitions", description = "Number of partitions for auto-created topic", defaultValue = "3")
    private int topicPartitions;

    @Inject
    private EventRouter eventRouter;

    @Inject
    private KafkaClientFactory kafkaClientFactory;

    @Inject
    private TypeManager typeManager;

    @Inject
    private Hostname hostname;

    private KafkaEventPublisher publisher;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        if (topicCreate) {
            kafkaClientFactory.ensureTopicExists(topic, topicPartitions, (short) 1);
        }

        var source = "edc://" + hostname.get();
        var objectMapper = typeManager.getMapper();

        var kafkaProducer = kafkaClientFactory.createProducer();
        publisher = new KafkaEventPublisher(kafkaProducer, topic, source, objectMapper, context.getMonitor());

        context.getMonitor().info("Kafka Event Publishing Extension initialized — publishing to topic: " + topic);
    }

    @Override
    public void prepare() {
        // Register for ALL event types — matching events-nats behavior
        eventRouter.register(Event.class, publisher);
    }

    @Override
    public void shutdown() {
        if (publisher != null) {
            publisher.close();
        }
    }
}
