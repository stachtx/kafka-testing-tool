package com.acme.kafkatester.codec;

import java.util.Locale;

/**
 * Supported wire formats. Constant names equal the catalog's {@code EventTypeDescriptor.format()}
 * strings ("AVRO" / "PROTOBUF"), so {@code MessageFormat.valueOf(descriptor.format())} works.
 */
public enum MessageFormat {
    AVRO,
    PROTOBUF;

    /** Case-insensitive lookup; throws {@link IllegalArgumentException} for unknown names. */
    public static MessageFormat parse(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Message format must not be null");
        }
        try {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported message format: " + name, e);
        }
    }
}
