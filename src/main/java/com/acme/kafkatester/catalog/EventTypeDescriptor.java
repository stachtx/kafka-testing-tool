package com.acme.kafkatester.catalog;

import java.util.List;

/**
 * Format-neutral view of a catalog event type.
 *
 * @param name         event type name (e.g. {@code PingEvent}); may exist in more than one format
 * @param format       wire format id, e.g. {@link #FORMAT_AVRO} / {@link #FORMAT_PROTOBUF}
 *                     (matches the names of the codec layer's MessageFormat enum constants)
 * @param fields       top-level fields
 * @param defaultTopic suggested topic, or {@code null} when the schema does not declare one
 * @param description human readable description, or {@code null}
 */
public record EventTypeDescriptor(
        String name,
        String format,
        List<Field> fields,
        String defaultTopic,
        String description) {

    public static final String FORMAT_AVRO = "AVRO";
    public static final String FORMAT_PROTOBUF = "PROTOBUF";

    public EventTypeDescriptor {
        fields = fields == null ? List.of() : List.copyOf(fields);
    }

    /** A top-level field: display type string and whether it may be absent/null. */
    public record Field(String name, String type, boolean nullable) {
    }
}
