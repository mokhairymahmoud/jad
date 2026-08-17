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

package org.eclipse.edc.jad.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.edc.spi.monitor.Monitor;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Factory for creating configured Kafka producers and consumers.
 * Encapsulates connection and security configuration.
 */
public class KafkaClientFactory {

    private final KafkaConfig config;
    private final Monitor monitor;

    public KafkaClientFactory(KafkaConfig config, Monitor monitor) {
        this.config = config;
        this.monitor = monitor;
    }

    /**
     * Creates a new Kafka producer with idempotent delivery enabled.
     */
    public KafkaProducer<String, String> createProducer() {
        var props = baseProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        monitor.debug("Creating Kafka producer for bootstrap servers: " + config.bootstrapServers());
        return new KafkaProducer<>(props);
    }

    /**
     * Creates a new Kafka consumer for the given group.
     *
     * @param groupId         consumer group ID
     * @param autoOffsetReset offset reset strategy (earliest/latest)
     */
    public KafkaConsumer<String, String> createConsumer(String groupId, String autoOffsetReset) {
        var props = baseProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        monitor.debug("Creating Kafka consumer for group: " + groupId);
        return new KafkaConsumer<>(props);
    }

    /**
     * Ensures a topic exists, creating it if necessary.
     *
     * @param topicName  the topic to create
     * @param partitions number of partitions
     * @param replicas   replication factor
     */
    public void ensureTopicExists(String topicName, int partitions, short replicas) {
        var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        applySecurity(adminProps);

        try (var admin = AdminClient.create(adminProps)) {
            var existingTopics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if (existingTopics.contains(topicName)) {
                monitor.debug("Kafka topic already exists: " + topicName);
                return;
            }

            var newTopic = new NewTopic(topicName, partitions, replicas);
            newTopic.configs(Map.of(
                    "cleanup.policy", "delete",
                    "retention.ms", "86400000" // 24 hours
            ));
            admin.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);
            monitor.info("Created Kafka topic: " + topicName + " (partitions=" + partitions + ", replicas=" + replicas + ")");
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            monitor.warning("Failed to create Kafka topic '" + topicName + "': " + e.getMessage());
        }
    }

    private Properties baseProperties() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());

        if (config.clientId() != null && !config.clientId().isBlank()) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, config.clientId());
        }

        applySecurity(props);
        return props;
    }

    private void applySecurity(Properties props) {
        if (config.securityProtocol() != null && !config.securityProtocol().isBlank()) {
            props.put("security.protocol", config.securityProtocol());
        }
        if (config.saslMechanism() != null && !config.saslMechanism().isBlank()) {
            props.put("sasl.mechanism", config.saslMechanism());
        }
        if (config.saslJaasConfig() != null && !config.saslJaasConfig().isBlank()) {
            props.put("sasl.jaas.config", config.saslJaasConfig());
        }
    }
}
