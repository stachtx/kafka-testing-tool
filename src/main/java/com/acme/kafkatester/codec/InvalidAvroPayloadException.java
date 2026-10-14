package com.acme.kafkatester.codec;

/** JSON payload does not conform to the Avro schema (error code INVALID_AVRO_PAYLOAD). */
public class InvalidAvroPayloadException extends InvalidPayloadException {

    public static final String ERROR_CODE = "INVALID_AVRO_PAYLOAD";

    public InvalidAvroPayloadException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public MessageFormat format() {
        return MessageFormat.AVRO;
    }

    @Override
    public String errorCode() {
        return ERROR_CODE;
    }
}
