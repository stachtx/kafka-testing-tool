package com.acme.kafkatester.producer;

/** Send to the DEV cluster failed (broker, timeout, or serializer/registry error). Maps to KAFKA_SEND_FAILED (502). */
public class KafkaSendFailedException extends RuntimeException {

    public static final String ERROR_CODE = "KAFKA_SEND_FAILED";

    public KafkaSendFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public String errorCode() {
        return ERROR_CODE;
    }
}
