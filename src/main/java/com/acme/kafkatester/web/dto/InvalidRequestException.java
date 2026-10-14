package com.acme.kafkatester.web.dto;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.consumer.ConsumerSession.OffsetMode;

/** Request failed pre-validation; maps to {@code VALIDATION_ERROR} / 400 with an {@code errors[]} entry. */
public class InvalidRequestException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static final Pattern TOPIC = Pattern.compile("[A-Za-z0-9._-]{1,249}");

    private final String field;

    public InvalidRequestException(String field, String message) {
        super(message);
        this.field = field;
    }

    public String field() {
        return field;
    }

    public List<FieldError> errors() {
        return List.of(new FieldError(field, getMessage()));
    }

    public record FieldError(String field, String message) {
    }

    // ---- shared pre-validation helpers --------------------------------------------------------

    public static String topic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new InvalidRequestException("topic", "topic must not be blank");
        }
        String t = topic.trim();
        if (!TOPIC.matcher(t).matches() || t.equals(".") || t.equals("..")) {
            throw new InvalidRequestException("topic",
                    "topic may only contain letters, digits, '.', '_' and '-' (max 249 characters)");
        }
        return t;
    }

    /** Required format. */
    public static MessageFormat format(String format) {
        if (format == null || format.isBlank()) {
            throw new InvalidRequestException("format", "format must be AVRO or PROTOBUF");
        }
        return optionalFormat(format);
    }

    /** Optional format: null/blank returns null. */
    public static MessageFormat optionalFormat(String format) {
        if (format == null || format.isBlank()) {
            return null;
        }
        try {
            return MessageFormat.parse(format);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("format", "format must be AVRO or PROTOBUF");
        }
    }

    public static OffsetMode offsetMode(String mode) {
        try {
            return OffsetMode.valueOf(mode == null ? "" : mode.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("offsetMode", "offsetMode must be LATEST or EARLIEST");
        }
    }
}
