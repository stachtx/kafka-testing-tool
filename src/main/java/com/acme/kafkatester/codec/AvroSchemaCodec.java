package com.acme.kafkatester.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.springframework.stereotype.Component;

/**
 * Avro codec over {@link GenericRecord} using Avro's native JSON encoding. Union values are
 * therefore written/accepted in Avro's verbose form (e.g. {@code {"string":"x"}}); generated
 * examples use {@code null} for nullable unions to keep them simple.
 */
@Component
public class AvroSchemaCodec implements SchemaCodec {

    private static final int MAX_EXAMPLE_DEPTH = 4;

    @Override
    public MessageFormat format() {
        return MessageFormat.AVRO;
    }

    @Override
    public Object encode(Object schema, String json) {
        Schema s = schema(schema);
        if (json == null || json.isBlank()) {
            throw new InvalidAvroPayloadException("Avro payload must not be empty", null);
        }
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(s, json);
            return new GenericDatumReader<Object>(s).read(null, decoder);
        } catch (IOException | RuntimeException e) {
            throw new InvalidAvroPayloadException(
                    "Payload does not match Avro schema " + s.getFullName() + ": " + e.getMessage() + hint(e), e);
        }
    }

    /** Actionable hint for the two classic Avro-JSON pitfalls: union branch wrappers and missing defaulted fields. */
    private static String hint(Exception e) {
        String m = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
        // a missing field surfaces as "got END_OBJECT" (or an unexpected field name) from the JSON decoder
        boolean missingField = m.contains("end_object") || m.contains("expected field name")
                || m.contains("missing");
        StringBuilder sb = new StringBuilder();
        if (missingField) {
            sb.append(" Hint: Avro JSON requires every field to be present, including fields that have a"
                    + " default (and nullable fields: use null).");
        } else if (m.contains("union")) {
            sb.append(" Hint: nullable/union fields must be null or wrapped with the branch type name,"
                    + " e.g. {\"string\":\"x\"} instead of \"x\"; use the full name for named types.");
        }
        return sb.toString();
    }

    @Override
    public String decode(Object schema, Object value) {
        Schema s = schema(schema);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().jsonEncoder(s, out);
            new GenericDatumWriter<Object>(s).write(value, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            throw new InvalidAvroPayloadException(
                    "Value cannot be rendered with Avro schema " + s.getFullName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public String generateExample(Object schema) {
        Schema s = schema(schema);
        return decode(s, exampleValue(s, 0));
    }

    private static Schema schema(Object schema) {
        if (schema instanceof Schema s) {
            return s;
        }
        throw new IllegalArgumentException("Avro codec requires an org.apache.avro.Schema but got "
                + (schema == null ? "null" : schema.getClass().getName()));
    }

    private Object exampleValue(Schema s, int depth) {
        return switch (s.getType()) {
            case RECORD -> {
                GenericRecord r = new GenericData.Record(s);
                for (Schema.Field f : s.getFields()) {
                    r.put(f.name(), exampleValue(f.schema(), depth + 1));
                }
                yield r;
            }
            case STRING -> "string";
            case BYTES -> ByteBuffer.wrap(new byte[] {0});
            case INT -> 0;
            case LONG -> 0L;
            case FLOAT -> 0.0f;
            case DOUBLE -> 0.0d;
            case BOOLEAN -> Boolean.FALSE;
            case NULL -> null;
            case ENUM -> new GenericData.EnumSymbol(s, s.getEnumSymbols().get(0));
            case FIXED -> new GenericData.Fixed(s, new byte[s.getFixedSize()]);
            case ARRAY -> {
                List<Object> list = new ArrayList<>();
                if (depth < MAX_EXAMPLE_DEPTH) {
                    list.add(exampleValue(s.getElementType(), depth + 1));
                }
                yield list;
            }
            case MAP -> {
                Map<String, Object> map = new HashMap<>();
                if (depth < MAX_EXAMPLE_DEPTH) {
                    map.put("key", exampleValue(s.getValueType(), depth + 1));
                }
                yield map;
            }
            case UNION -> {
                // prefer null (simple JSON); otherwise first branch
                Schema pick = s.getTypes().stream()
                        .filter(t -> t.getType() == Schema.Type.NULL)
                        .findFirst()
                        .orElse(s.getTypes().get(0));
                yield exampleValue(pick, depth + 1);
            }
        };
    }
}
