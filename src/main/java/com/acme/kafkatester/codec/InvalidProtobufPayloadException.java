package com.acme.kafkatester.codec;

/** JSON payload does not conform to the Protobuf message (error code INVALID_PROTOBUF_PAYLOAD). */
public class InvalidProtobufPayloadException extends InvalidPayloadException {

    public static final String ERROR_CODE = "INVALID_PROTOBUF_PAYLOAD";

    public InvalidProtobufPayloadException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public MessageFormat format() {
        return MessageFormat.PROTOBUF;
    }

    @Override
    public String errorCode() {
        return ERROR_CODE;
    }
}
