package com.arun.kafkaviewer.api;

public record TopicSummary(String name, int partitionCount, boolean internal) {
}
