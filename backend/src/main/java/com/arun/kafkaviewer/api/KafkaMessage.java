package com.arun.kafkaviewer.api;

import java.time.Instant;
import java.util.List;

public record KafkaMessage(
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        String timestampType,
        Payload key,
        Payload value,
        List<MessageHeader> headers) {

    public record Payload(String text, String base64, int size) {
    }

    public record MessageHeader(String key, String text, String base64) {
    }
}
