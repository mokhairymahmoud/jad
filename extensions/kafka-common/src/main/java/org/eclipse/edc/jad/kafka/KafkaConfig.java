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

import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.runtime.metamodel.annotation.Settings;

/**
 * Shared Kafka connection configuration used by all Kafka extensions.
 */
@Settings
public record KafkaConfig(

        @Setting(key = "edc.kafka.bootstrap.servers", description = "Kafka bootstrap servers (comma-separated)", required = true)
        String bootstrapServers,

        @Setting(key = "edc.kafka.security.protocol", description = "Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)", defaultValue = "PLAINTEXT")
        String securityProtocol,

        @Setting(key = "edc.kafka.client.id", description = "Kafka client ID (defaults to EDC runtime ID)", required = false)
        String clientId,

        @Setting(key = "edc.kafka.sasl.mechanism", description = "SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", required = false)
        String saslMechanism,

        @Setting(key = "edc.kafka.sasl.jaas.config", description = "SASL JAAS configuration string", required = false)
        String saslJaasConfig
) {
}
