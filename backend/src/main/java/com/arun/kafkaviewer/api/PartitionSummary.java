package com.arun.kafkaviewer.api;

public record PartitionSummary(int partition, long beginningOffset, long endOffset, long messageCount) {
}
