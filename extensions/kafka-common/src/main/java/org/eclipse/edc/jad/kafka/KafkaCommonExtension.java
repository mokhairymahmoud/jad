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

import org.eclipse.edc.runtime.metamodel.annotation.Configuration;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

import static org.eclipse.edc.jad.kafka.KafkaCommonExtension.NAME;

/**
 * Provides a shared {@link KafkaClientFactory} to all Kafka-based extensions.
 * The factory is registered as a service and injected by downstream extensions.
 */
@Extension(NAME)
public class KafkaCommonExtension implements ServiceExtension {
    public static final String NAME = "Kafka Common Extension";

    @Configuration
    private KafkaConfig kafkaConfig;

    @Override
    public String name() {
        return NAME;
    }

    @Provider
    public KafkaClientFactory kafkaClientFactory(ServiceExtensionContext context) {
        // Use runtime ID as default client ID if not explicitly configured
        var effectiveConfig = kafkaConfig;
        if (effectiveConfig.clientId() == null || effectiveConfig.clientId().isBlank()) {
            effectiveConfig = new KafkaConfig(
                    kafkaConfig.bootstrapServers(),
                    kafkaConfig.securityProtocol(),
                    context.getRuntimeId(),
                    kafkaConfig.saslMechanism(),
                    kafkaConfig.saslJaasConfig()
            );
        }
        return new KafkaClientFactory(effectiveConfig, context.getMonitor());
    }
}
