package com.arun.kafkaviewer.service;

import org.springframework.http.HttpStatus;

public class KafkaViewerException extends RuntimeException {

    private final HttpStatus status;

    public KafkaViewerException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public KafkaViewerException(HttpStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public HttpStatus status() {
        return status;
    }
}
