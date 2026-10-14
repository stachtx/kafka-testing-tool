package com.acme.kafkatester.catalog.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import com.acme.kafkatester.catalog.CatalogProvider;
import com.acme.kafkatester.catalog.DefaultTopics;
import com.acme.kafkatester.catalog.EventTypeDescriptor;
import com.acme.kafkatester.catalog.UnknownEventTypeException;
import com.acme.kafkatester.codec.AvroSchemaCodec;

/**
 * Discovers every {@code classpath*:schemas/avro/*.avsc} at startup and caches the parsed
 * {@link Schema}s. The suggested topic comes from {@code schemas/default-topics.properties}
 * ({@link DefaultTopics}); a legacy top-level {@code "defaultTopic"} property is a fallback only and is
 * stripped before the schema is previewed/registered. Duplicate simple names fail startup.
 */
@Component
@Order(1)
public class AvroCatalogProvider implements CatalogProvider {

    public static final String DEFAULT_LOCATION_PATTERN = "classpath*:schemas/avro/*.avsc";
    static final String DEFAULT_TOPIC_PROP = "defaultTopic";

    private final Map<String, Schema> schemas = new TreeMap<>();
    private final AvroSchemaCodec codec = new AvroSchemaCodec();

    private final DefaultTopics defaultTopics;

    public AvroCatalogProvider() {
        this(DEFAULT_LOCATION_PATTERN, DefaultTopics.load());
    }

    AvroCatalogProvider(String locationPattern, DefaultTopics defaultTopics) {
        this.defaultTopics = defaultTopics;
        try {
            for (Resource r : new PathMatchingResourcePatternResolver().getResources(locationPattern)) {
                try (InputStream in = r.getInputStream()) {
                    // fresh parser per file so files do not collide on shared type names
                    Schema schema = new Schema.Parser().parse(in);
                    if (schema.getType() == Schema.Type.RECORD) {
                        Schema previous = schemas.put(schema.getName(), schema);
                        if (previous != null) {
                            throw new IllegalStateException("Duplicate Avro event type name '" + schema.getName()
                                    + "' (" + previous.getFullName() + " and " + schema.getFullName() + ", latest in "
                                    + r.getDescription() + "); the catalog is keyed by simple name, rename one");
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load Avro schemas from " + locationPattern, e);
        }
    }

    @Override
    public String format() {
        return EventTypeDescriptor.FORMAT_AVRO;
    }

    @Override
    public List<EventTypeDescriptor> listEventTypes() {
        return schemas.values().stream().map(this::describe).toList();
    }

    @Override
    public Optional<EventTypeDescriptor> resolve(String name) {
        return Optional.ofNullable(schemas.get(name)).map(this::describe);
    }

    /** Parsed schema for codec use; empty if unknown. */
    public Optional<Schema> schema(String name) {
        return Optional.ofNullable(schemas.get(name));
    }

    @Override
    public Optional<Object> nativeSchema(String name) {
        return schema(name).map(s -> (Object) s);
    }

    /** Example JSON payload for the named record, produced by the Avro codec. */
    @Override
    public String generateExample(String name) {
        Schema schema = schemas.get(name);
        if (schema == null) {
            throw new UnknownEventTypeException(name, format());
        }
        return codec.generateExample(schema);
    }

    private EventTypeDescriptor describe(Schema schema) {
        List<EventTypeDescriptor.Field> fields = new ArrayList<>();
        for (Schema.Field f : schema.getFields()) {
            Schema type = f.schema();
            boolean nullable = false;
            if (type.getType() == Schema.Type.UNION) {
                nullable = type.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
                type = type.getTypes().stream()
                        .filter(t -> t.getType() != Schema.Type.NULL)
                        .findFirst()
                        .orElse(type);
            }
            fields.add(new EventTypeDescriptor.Field(f.name(), typeName(type), nullable));
        }
        // sidecar wins; the legacy in-schema property is still honoured (and stripped before registration)
        Object legacy = schema.getObjectProp(DEFAULT_TOPIC_PROP);
        String topic = defaultTopics.forEvent(schema.getName())
                .orElse(legacy == null ? null : legacy.toString());
        return new EventTypeDescriptor(schema.getName(), EventTypeDescriptor.FORMAT_AVRO, fields,
                topic, schema.getDoc());
    }

    private static String typeName(Schema s) {
        return switch (s.getType()) {
            case ARRAY -> "array<" + typeName(s.getElementType()) + ">";
            case MAP -> "map<" + typeName(s.getValueType()) + ">";
            case RECORD, ENUM, FIXED -> s.getFullName();
            default -> s.getType().getName();
        };
    }
}
