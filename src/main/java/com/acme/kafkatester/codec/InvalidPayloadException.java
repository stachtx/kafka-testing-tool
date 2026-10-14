package com.acme.kafkatester.codec;

/**
 * Base for payloads that do not conform to the target schema. Subtypes map to the error codes
 * {@code INVALID_AVRO_PAYLOAD} / {@code INVALID_PROTOBUF_PAYLOAD} in the REST layer. The message
 * is safe to show to users (no stack trace, no credentials).
 */
public abstract class InvalidPayloadException extends RuntimeException {

    protected InvalidPayloadException(String message, Throwable cause) {
        super(message, cause);
    }

    public abstract MessageFormat format();

    /** Taxonomy code for the error response. */
    public abstract String errorCode();
}
