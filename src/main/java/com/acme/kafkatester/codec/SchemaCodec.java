package com.acme.kafkatester.codec;

/**
 * Per-format strategy converting between JSON text and the schema-driven in-memory value that
 * the Confluent serializers accept ({@code GenericRecord} for Avro, {@code DynamicMessage} for
 * Protobuf; ADR-006, no generated classes).
 *
 * <p>The schema is always supplied by the caller: an {@code org.apache.avro.Schema} for Avro or a
 * {@code com.google.protobuf.Descriptors.Descriptor} for Protobuf. The codec never looks schemas
 * up itself, so producers pass the catalog's schema and consumers pass the registry-resolved one.
 * A schema of the wrong type raises {@link IllegalArgumentException}.
 */
public interface SchemaCodec {

    MessageFormat format();

    /**
     * Parse JSON into a schema-conformant value.
     *
     * @throws InvalidPayloadException (format-specific subtype) when the JSON does not conform
     */
    Object encode(Object schema, String json);

    /** Render a value (as produced by {@link #encode} or by a deserializer) as JSON text. */
    String decode(Object schema, Object value);

    /** Build a sample JSON payload that {@link #encode} accepts for the schema. */
    String generateExample(Object schema);
}
