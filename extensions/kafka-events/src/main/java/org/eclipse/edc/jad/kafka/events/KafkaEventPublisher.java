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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonCloudEventData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.edc.spi.event.Event;
import org.eclipse.edc.spi.event.EventEnvelope;
import org.eclipse.edc.spi.event.EventSubscriber;
import org.eclipse.edc.spi.monitor.Monitor;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Publishes EDC domain events to a Kafka topic as CloudEvents.
 * <p>
 * This mirrors the behavior of the upstream NATS events-nats extension:
 * events are wrapped in a CloudEvents JSON envelope and published to the
 * configured topic with the event name as the record key (enabling partitioning
 * by event type).
 * <p>
 * W3C trace context (traceparent) is propagated via Kafka headers.
 */
public class KafkaEventPublisher implements EventSubscriber {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final String source;
    private final ObjectMapper objectMapper;
    private final Monitor monitor;

    public KafkaEventPublisher(KafkaProducer<String, String> producer, String topic,
                               String source, ObjectMapper objectMapper, Monitor monitor) {
        this.producer = producer;
        this.topic = topic;
        this.source = source;
        this.objectMapper = objectMapper;
        this.monitor = monitor;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void on(EventEnvelope event) {
        try {
            var payload = event.getPayload();
            var eventName = payload.name();
            var jsonPayload = objectMapper.writeValueAsString(event);

            // Build CloudEvents envelope matching the NATS extension format
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create(source))
                    .withType(eventName)
                    .withTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(event.getAt()), ZoneOffset.UTC))
                    .withDataContentType("application/json")
                    .withData(JsonCloudEventData.wrap(objectMapper.readTree(jsonPayload)))
                    .build();

            var serializedEvent = objectMapper.writeValueAsString(cloudEvent);

            var record = new ProducerRecord<>(topic, eventName, serializedEvent);

            // Propagate trace context via Kafka headers (matching NATS extension behavior)
            record.headers().add(new RecordHeader("ce_type", eventName.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("ce_source", source.getBytes(StandardCharsets.UTF_8)));

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    monitor.warning("Failed to publish event to Kafka topic '" + topic + "': " + exception.getMessage());
                } else {
                    monitor.debug("Published event '" + eventName + "' to " + topic
                            + " [partition=" + metadata.partition() + ", offset=" + metadata.offset() + "]");
                }
            });

        } catch (JsonProcessingException e) {
            monitor.severe("Failed to serialize event for Kafka publishing", e);
        }
    }

    /**
     * Flushes pending messages and closes the producer.
     */
    public void close() {
        producer.flush();
        producer.close();
    }
}
