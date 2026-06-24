package com.arun.kafkaviewer.api;

import com.arun.kafkaviewer.service.KafkaViewerException;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Instant;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(KafkaViewerException.class)
    ResponseEntity<ApiError> handleKafkaViewerException(
            KafkaViewerException exception, HttpServletRequest request) {
        return ResponseEntity.status(exception.status())
                .body(new ApiError(
                        Instant.now(),
                        exception.status().value(),
                        exception.status().getReasonPhrase(),
                        exception.getMessage(),
                        request.getRequestURI()));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    ResponseEntity<ApiError> handleMissingParameter(
            MissingServletRequestParameterException exception, HttpServletRequest request) {
        return ResponseEntity.badRequest()
                .body(new ApiError(
                        Instant.now(),
                        400,
                        "Bad Request",
                        "Required parameter '%s' is missing".formatted(exception.getParameterName()),
                        request.getRequestURI()));
    }

    record ApiError(Instant timestamp, int status, String error, String message, String path) {
    }
}
