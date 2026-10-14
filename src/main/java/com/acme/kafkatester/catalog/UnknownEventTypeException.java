package com.acme.kafkatester.catalog;

/** Thrown when an event type is not in the catalog. REST layer maps this to UNKNOWN_EVENT_TYPE (404). */
public class UnknownEventTypeException extends RuntimeException {

    private final String eventType;

    public UnknownEventTypeException(String eventType) {
        super("Unknown event type: " + eventType);
        this.eventType = eventType;
    }

    public UnknownEventTypeException(String eventType, String format) {
        super("Unknown event type: " + eventType + " (format " + format + ")");
        this.eventType = eventType;
    }

    public String getEventType() {
        return eventType;
    }
}
