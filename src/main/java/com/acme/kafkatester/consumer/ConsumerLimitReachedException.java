package com.acme.kafkatester.consumer;

/** Too many concurrent consumer sessions. Maps to {@code CONSUMER_LIMIT_REACHED} / HTTP 429. */
public class ConsumerLimitReachedException extends RuntimeException {

    public static final String ERROR_CODE = "CONSUMER_LIMIT_REACHED";

    private static final long serialVersionUID = 1L;

    private final int maxSessions;

    public ConsumerLimitReachedException(int maxSessions) {
        super("Maximum number of consumer sessions (" + maxSessions + ") reached. Stop an existing session first.");
        this.maxSessions = maxSessions;
    }

    public int maxSessions() {
        return maxSessions;
    }
}
