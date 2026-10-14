package com.acme.kafkatester.consumer;

/**
 * Unknown or already-stopped consumer session id. Maps to {@code CONSUMER_SESSION_NOT_FOUND} / HTTP 404
 * for both poll and stop.
 */
public class ConsumerSessionNotFoundException extends RuntimeException {

    public static final String ERROR_CODE = "CONSUMER_SESSION_NOT_FOUND";

    private final String sessionId;

    public ConsumerSessionNotFoundException(String sessionId) {
        super("Consumer session not found: " + sessionId);
        this.sessionId = sessionId;
    }

    public String sessionId() {
        return sessionId;
    }
}
