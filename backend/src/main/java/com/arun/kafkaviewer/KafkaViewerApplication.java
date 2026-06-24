package com.arun.kafkaviewer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaViewerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaViewerApplication.class, args);
    }
}
