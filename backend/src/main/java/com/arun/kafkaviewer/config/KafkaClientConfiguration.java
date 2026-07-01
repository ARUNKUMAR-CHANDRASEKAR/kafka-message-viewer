package com.arun.kafkaviewer.config;

import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class KafkaClientConfiguration {

    @Bean(destroyMethod = "close")
    Admin viewerAdmin(KafkaProperties properties) {
        return Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", properties.getBootstrapServers())));
    }

    @Bean
    ConsumerFactory<byte[], byte[]> kafkaConsumerFactory(KafkaProperties properties) {
        return new DefaultKafkaConsumerFactory<>(
                properties.buildConsumerProperties(null),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    }
}
