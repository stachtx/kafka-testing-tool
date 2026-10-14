package com.acme.kafkatester.catalog.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import com.acme.kafkatester.catalog.CatalogProvider;
import com.acme.kafkatester.catalog.DefaultTopics;
import com.acme.kafkatester.catalog.EventTypeDescriptor;
import com.acme.kafkatester.catalog.UnknownEventTypeException;
import com.acme.kafkatester.codec.ProtobufSchemaCodec;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

/**
 * Loads the build-generated {@code FileDescriptorSet} (classpath {@code main.desc}) and caches
 * the top-level message {@link Descriptor}s. No generated classes exist (ADR-006).
 *
 * <p>Files under {@code google/} (well-known imports bundled via {@code includeImports}) are
 * not catalogued. The suggested topic comes from {@code schemas/default-topics.properties}
 * ({@link DefaultTopics}), so no tool-private option lives in the registered descriptor. Missing
 * descriptor dependencies and duplicate simple message names fail startup.
 */
@Component
@Order(2)
public class ProtobufCatalogProvider implements CatalogProvider {

    public static final String DEFAULT_DESCRIPTOR_RESOURCE = "main.desc";

    private final Map<String, Descriptor> messages = new TreeMap<>();
    private final ProtobufSchemaCodec codec = new ProtobufSchemaCodec();

    private final DefaultTopics defaultTopics;

    public ProtobufCatalogProvider() {
        this(DEFAULT_DESCRIPTOR_RESOURCE, DefaultTopics.load());
    }

    ProtobufCatalogProvider(String descriptorResource, DefaultTopics defaultTopics) {
        this.defaultTopics = defaultTopics;
        Resource resource = new ClassPathResource(descriptorResource);
        if (!resource.exists()) {
            return; // no protobuf schemas packaged: empty catalog
        }
        try (InputStream in = resource.getInputStream()) {
            load(FileDescriptorSet.parseFrom(in), descriptorResource);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load protobuf descriptor set " + descriptorResource, e);
        }
    }

    ProtobufCatalogProvider(FileDescriptorSet set, DefaultTopics defaultTopics) {
        this.defaultTopics = defaultTopics;
        load(set, "<in-memory descriptor set>");
    }

    private void load(FileDescriptorSet set, String source) {
        try {
            Map<String, FileDescriptor> built = new HashMap<>();
            for (FileDescriptorProto proto : set.getFileList()) {
                List<FileDescriptor> deps = new ArrayList<>();
                for (String dep : proto.getDependencyList()) {
                    FileDescriptor d = built.get(dep);
                    if (d == null) {
                        throw new IllegalStateException("Protobuf file '" + proto.getName() + "' in " + source
                                + " depends on '" + dep + "' which is missing from the descriptor set"
                                + " (or listed after it); build with includeImports");
                    }
                    deps.add(d);
                }
                FileDescriptor fd = FileDescriptor.buildFrom(proto, deps.toArray(FileDescriptor[]::new));
                built.put(proto.getName(), fd);
                if (!proto.getName().startsWith("google/")) {
                    for (Descriptor m : fd.getMessageTypes()) {
                        Descriptor previous = messages.put(m.getName(), m);
                        if (previous != null) {
                            throw new IllegalStateException("Duplicate Protobuf event type name '" + m.getName()
                                    + "' (" + previous.getFullName() + " and " + m.getFullName()
                                    + "); the catalog is keyed by simple name, rename one");
                        }
                    }
                }
            }
        } catch (DescriptorValidationException e) {
            throw new IllegalStateException("Invalid protobuf descriptor set " + source, e);
        }
    }

    @Override
    public String format() {
        return EventTypeDescriptor.FORMAT_PROTOBUF;
    }

    @Override
    public List<EventTypeDescriptor> listEventTypes() {
        return messages.values().stream().map(this::describe).toList();
    }

    @Override
    public Optional<EventTypeDescriptor> resolve(String name) {
        return Optional.ofNullable(messages.get(name)).map(this::describe);
    }

    /** Message descriptor for codec use; empty if unknown. */
    public Optional<Descriptor> descriptor(String name) {
        return Optional.ofNullable(messages.get(name));
    }

    @Override
    public Optional<Object> nativeSchema(String name) {
        return descriptor(name).map(d -> (Object) d);
    }

    /** Example JSON payload for the named message, produced by the Protobuf codec. */
    @Override
    public String generateExample(String name) {
        Descriptor d = messages.get(name);
        if (d == null) {
            throw new UnknownEventTypeException(name, format());
        }
        return codec.generateExample(d);
    }

    private EventTypeDescriptor describe(Descriptor d) {
        List<EventTypeDescriptor.Field> fields = new ArrayList<>();
        for (FieldDescriptor f : d.getFields()) {
            boolean nullable = f.hasPresence() && !f.isRepeated();
            fields.add(new EventTypeDescriptor.Field(f.getName(), typeName(f), nullable));
        }
        return new EventTypeDescriptor(d.getName(), EventTypeDescriptor.FORMAT_PROTOBUF, fields,
                defaultTopics.forEvent(d.getName()).orElse(null), null);
    }

    private static String typeName(FieldDescriptor f) {
        String base = switch (f.getJavaType()) {
            case MESSAGE -> f.getMessageType().getFullName();
            case ENUM -> f.getEnumType().getFullName();
            default -> f.getType().name().toLowerCase();
        };
        if (f.isMapField()) {
            return "map<" + typeName(f.getMessageType().findFieldByName("value")) + ">";
        }
        return f.isRepeated() ? "repeated " + base : base;
    }
}
