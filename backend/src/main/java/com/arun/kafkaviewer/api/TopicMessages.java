package com.arun.kafkaviewer.api;

import java.util.List;

public record TopicMessages(
        String topic,
        Integer partition,
        int limit,
        List<KafkaMessage> messages) {
}
