package com.acme.kafkatester.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.springframework.stereotype.Component;

/**
 * Protobuf codec over {@link DynamicMessage} and {@link JsonFormat} (no generated classes).
 * JSON uses the proto field names (e.g. {@code sent_at}); the parser also accepts lowerCamelCase.
 */
@Component
public class ProtobufSchemaCodec implements SchemaCodec {

    private static final int MAX_EXAMPLE_DEPTH = 3;

    @Override
    public MessageFormat format() {
        return MessageFormat.PROTOBUF;
    }

    @Override
    public Object encode(Object schema, String json) {
        Descriptor d = descriptor(schema);
        if (json == null || json.isBlank()) {
            throw new InvalidProtobufPayloadException("Protobuf payload must not be empty", null);
        }
        try {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(d);
            JsonFormat.parser().merge(json, builder);
            return builder.build();
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            throw new InvalidProtobufPayloadException(
                    "Payload does not match Protobuf message " + d.getFullName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public String decode(Object schema, Object value) {
        Descriptor d = descriptor(schema);
        try {
            if (!(value instanceof Message m)) {
                throw new IllegalArgumentException("Expected a protobuf Message but got "
                        + (value == null ? "null" : value.getClass().getName()));
            }
            return JsonFormat.printer().preservingProtoFieldNames().print(m);
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            throw new InvalidProtobufPayloadException(
                    "Value cannot be rendered as Protobuf message " + d.getFullName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public String generateExample(Object schema) {
        Descriptor d = descriptor(schema);
        return decode(d, exampleMessage(d, 0));
    }

    private static Descriptor descriptor(Object schema) {
        if (schema instanceof Descriptor d) {
            return d;
        }
        throw new IllegalArgumentException("Protobuf codec requires a Descriptors.Descriptor but got "
                + (schema == null ? "null" : schema.getClass().getName()));
    }

    private DynamicMessage exampleMessage(Descriptor d, int depth) {
        DynamicMessage.Builder b = DynamicMessage.newBuilder(d);
        for (FieldDescriptor f : d.getFields()) {
            if (f.isMapField()) {
                continue; // keep examples simple: empty map
            }
            if (f.getJavaType() == FieldDescriptor.JavaType.MESSAGE && depth >= MAX_EXAMPLE_DEPTH) {
                continue;
            }
            if (f.isRepeated()) {
                b.addRepeatedField(f, exampleScalar(f, depth));
            } else {
                b.setField(f, exampleScalar(f, depth));
            }
        }
        return b.build();
    }

    private Object exampleScalar(FieldDescriptor f, int depth) {
        return switch (f.getJavaType()) {
            case STRING -> "string";
            case INT -> 0;
            case LONG -> 0L;
            case FLOAT -> 0.0f;
            case DOUBLE -> 0.0d;
            case BOOLEAN -> Boolean.FALSE;
            case BYTE_STRING -> ByteString.copyFrom(new byte[] {0});
            case ENUM -> f.getEnumType().getValues().get(0);
            case MESSAGE -> exampleMessage(f.getMessageType(), depth + 1);
        };
    }
}
