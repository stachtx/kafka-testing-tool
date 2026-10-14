package com.acme.kafkatester.consumer;

/** A consumer session could not be started. Maps to {@code CONSUMER_START_FAILED}. */
public class ConsumerStartException extends RuntimeException {

    public static final String ERROR_CODE = "CONSUMER_START_FAILED";

    public ConsumerStartException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConsumerStartException(String message) {
        super(message);
    }
}
