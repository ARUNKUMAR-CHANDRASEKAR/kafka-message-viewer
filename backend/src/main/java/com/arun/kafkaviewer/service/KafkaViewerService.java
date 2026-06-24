package com.arun.kafkaviewer.service;

import com.arun.kafkaviewer.api.KafkaMessage;
import com.arun.kafkaviewer.api.PartitionSummary;
import com.arun.kafkaviewer.api.TopicMessages;
import com.arun.kafkaviewer.api.TopicSummary;
import com.arun.kafkaviewer.config.KafkaViewerProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaViewerService {

    private final Admin admin;
    private final ConsumerFactory<byte[], byte[]> consumerFactory;
    private final KafkaViewerProperties properties;

    public KafkaViewerService(
            Admin admin,
            ConsumerFactory<byte[], byte[]> consumerFactory,
            KafkaViewerProperties properties) {
        this.admin = admin;
        this.consumerFactory = consumerFactory;
        this.properties = properties;
    }

    public List<TopicSummary> topics(boolean includeInternal) {
        try {
            var options = new org.apache.kafka.clients.admin.ListTopicsOptions().listInternal(includeInternal);
            var listings = admin.listTopics(options).listings().get();
            var names = listings.stream().map(listing -> listing.name()).toList();
            Map<String, TopicDescription> descriptions = names.isEmpty()
                    ? Map.of()
                    : admin.describeTopics(names).allTopicNames().get();

            return listings.stream()
                    .map(listing -> new TopicSummary(
                            listing.name(),
                            descriptions.get(listing.name()).partitions().size(),
                            listing.isInternal()))
                    .sorted(Comparator.comparing(TopicSummary::name))
                    .toList();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw unavailable("Interrupted while reading Kafka topics", exception);
        } catch (ExecutionException exception) {
            throw unavailable("Could not read Kafka topics", exception.getCause());
        }
    }

    public List<PartitionSummary> partitions(String topic) {
        validateTopicName(topic);
        try (Consumer<byte[], byte[]> consumer = consumerFactory.createConsumer()) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, properties.pollTimeout());
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                throw notFound(topic);
            }
            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(topicPartitions);

            return topicPartitions.stream()
                    .map(partition -> new PartitionSummary(
                            partition.partition(),
                            beginnings.get(partition),
                            ends.get(partition),
                            Math.max(0, ends.get(partition) - beginnings.get(partition))))
                    .sorted(Comparator.comparingInt(PartitionSummary::partition))
                    .toList();
        } catch (KafkaViewerException exception) {
            throw exception;
        } catch (Exception exception) {
            throw unavailable("Could not read partitions for topic '%s'".formatted(topic), exception);
        }
    }

    public TopicMessages latestMessages(String topic, Integer partition, int limit) {
        validateTopicName(topic);
        if (partition != null && partition < 0) {
            throw new KafkaViewerException(HttpStatus.BAD_REQUEST, "partition must be zero or greater");
        }
        if (limit < 1 || limit > properties.maxPageSize()) {
            throw new KafkaViewerException(
                    HttpStatus.BAD_REQUEST,
                    "limit must be between 1 and %d".formatted(properties.maxPageSize()));
        }

        try (Consumer<byte[], byte[]> consumer = consumerFactory.createConsumer()) {
            List<TopicPartition> selectedPartitions = selectedPartitions(consumer, topic, partition);
            Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(selectedPartitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(selectedPartitions);
            consumer.assign(selectedPartitions);
            selectedPartitions.forEach(selected ->
                    consumer.seek(selected, Math.max(beginnings.get(selected), ends.get(selected) - limit)));

            List<KafkaMessage> messages = new ArrayList<>();
            Instant deadline = Instant.now().plus(properties.pollTimeout());
            while (!atSnapshotEnd(consumer, selectedPartitions, ends) && Instant.now().isBefore(deadline)) {
                Duration remaining = Duration.between(Instant.now(), deadline);
                if (remaining.isNegative() || remaining.isZero()) {
                    break;
                }
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(remaining)) {
                    TopicPartition recordPartition = new TopicPartition(record.topic(), record.partition());
                    if (record.offset() < ends.get(recordPartition)) {
                        messages.add(toMessage(record));
                    }
                }
            }
            List<KafkaMessage> latest = messages.stream()
                    .sorted(Comparator.comparing(KafkaMessage::timestamp)
                            .thenComparingInt(KafkaMessage::partition)
                            .thenComparingLong(KafkaMessage::offset)
                            .reversed())
                    .limit(limit)
                    .toList();
            return new TopicMessages(topic, partition, limit, latest);
        } catch (KafkaViewerException exception) {
            throw exception;
        } catch (Exception exception) {
            throw unavailable("Could not read messages from topic '%s'".formatted(topic), exception);
        }
    }

    private List<TopicPartition> selectedPartitions(
            Consumer<byte[], byte[]> consumer, String topic, Integer partition) {
        List<PartitionInfo> partitions = consumer.partitionsFor(topic, properties.pollTimeout());
        if (partitions == null || partitions.isEmpty()) {
            throw notFound(topic);
        }
        if (partition != null && partitions.stream().noneMatch(info -> info.partition() == partition)) {
            throw new KafkaViewerException(
                    HttpStatus.NOT_FOUND,
                    "Partition %d does not exist in topic '%s'".formatted(partition, topic));
        }
        return partitions.stream()
                .filter(info -> partition == null || info.partition() == partition)
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();
    }

    private boolean atSnapshotEnd(
            Consumer<byte[], byte[]> consumer,
            List<TopicPartition> partitions,
            Map<TopicPartition, Long> ends) {
        return partitions.stream().allMatch(partition -> consumer.position(partition) >= ends.get(partition));
    }

    private void validateTopicName(String topic) {
        if (topic == null || topic.length() > 249 || !topic.matches("[a-zA-Z0-9._-]+")) {
            throw new KafkaViewerException(HttpStatus.BAD_REQUEST, "Invalid Kafka topic name");
        }
    }

    private KafkaMessage toMessage(ConsumerRecord<byte[], byte[]> record) {
        var headers = new ArrayList<KafkaMessage.MessageHeader>();
        record.headers().forEach(header -> headers.add(PayloadCodec.header(header.key(), header.value())));
        return new KafkaMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                record.timestampType().name(),
                PayloadCodec.payload(record.key()),
                PayloadCodec.payload(record.value()),
                List.copyOf(headers));
    }

    private KafkaViewerException notFound(String topic) {
        return new KafkaViewerException(HttpStatus.NOT_FOUND, "Topic '%s' does not exist".formatted(topic));
    }

    private KafkaViewerException unavailable(String message, Throwable cause) {
        if (cause instanceof UnknownTopicOrPartitionException) {
            return new KafkaViewerException(HttpStatus.NOT_FOUND, cause.getMessage(), cause);
        }
        return new KafkaViewerException(HttpStatus.SERVICE_UNAVAILABLE, message, cause);
    }
}
