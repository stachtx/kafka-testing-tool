package com.acme.kafkatester.registry;

import java.io.IOException;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.acme.kafkatester.catalog.EventTypeDescriptor;
import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.catalog.UnknownEventTypeException;
import com.acme.kafkatester.catalog.avro.AvroCatalogProvider;
import com.acme.kafkatester.catalog.protobuf.ProtobufCatalogProvider;
import com.acme.kafkatester.codec.MessageFormat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

/**
 * Turns a catalog event type into the registry client's {@link ParsedSchema}. Package-private
 * helper shared by the Gateway (preview) and the Service (register); contains no diff logic and
 * never calls the registry.
 */
@Component
class LocalSchemas {

    private final SchemaCatalog catalog;
    private final AvroCatalogProvider avro;
    private final ProtobufCatalogProvider protobuf;

    LocalSchemas(SchemaCatalog catalog, AvroCatalogProvider avro, ProtobufCatalogProvider protobuf) {
        this.catalog = catalog;
        this.avro = avro;
        this.protobuf = protobuf;
    }

    /** Resolves the descriptor; {@code format} may be null (first match). */
    EventTypeDescriptor describe(String eventType, String format) {
        return format == null || format.isBlank() ? catalog.resolve(eventType) : catalog.resolve(eventType, format);
    }

    /** Avro top-level properties that are tool-private and must never reach the shared registry (W6). */
    static final Set<String> TOOL_PRIVATE_AVRO_PROPS = Set.of("defaultTopic");

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * The schema exactly as it is previewed, compared and registered: tool-private metadata removed.
     * Protobuf needs no stripping because the default-topic convention lives in a sidecar file
     * ({@code schemas/default-topics.properties}), not in the descriptor.
     */
    ParsedSchema parsed(EventTypeDescriptor d) {
        MessageFormat format = MessageFormat.valueOf(d.format());
        return switch (format) {
            case AVRO -> new AvroSchema(stripPrivateAvroProps(avro.schema(d.name())
                    .orElseThrow(() -> new UnknownEventTypeException(d.name(), d.format()))));
            case PROTOBUF -> new ProtobufSchema(protobuf.descriptor(d.name())
                    .orElseThrow(() -> new UnknownEventTypeException(d.name(), d.format())));
        };
    }

    static org.apache.avro.Schema stripPrivateAvroProps(org.apache.avro.Schema schema) {
        if (TOOL_PRIVATE_AVRO_PROPS.stream().noneMatch(p -> schema.getObjectProp(p) != null)) {
            return schema;
        }
        try {
            ObjectNode node = (ObjectNode) JSON.readTree(schema.toString());
            TOOL_PRIVATE_AVRO_PROPS.forEach(node::remove);
            return new org.apache.avro.Schema.Parser().parse(node.toString());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot strip tool-private properties from Avro schema "
                    + schema.getFullName(), e);
        }
    }

    /** Human-readable normalized text for a diff view. */
    static String display(ParsedSchema schema) {
        if (schema.rawSchema() instanceof org.apache.avro.Schema avroSchema) {
            return avroSchema.toString(true);
        }
        return schema.canonicalString();
    }
}
