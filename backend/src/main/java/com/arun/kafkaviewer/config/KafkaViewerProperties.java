package com.arun.kafkaviewer.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka-viewer")
public record KafkaViewerProperties(Duration pollTimeout, int maxPageSize) {

    public KafkaViewerProperties {
        if (pollTimeout == null || pollTimeout.isNegative() || pollTimeout.isZero()) {
            pollTimeout = Duration.ofSeconds(2);
        }
        if (maxPageSize < 1) {
            maxPageSize = 500;
        }
    }
}
