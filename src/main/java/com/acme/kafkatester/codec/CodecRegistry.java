package com.acme.kafkatester.codec;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * Single point of format dispatch: every other class asks this registry for the codec (or uses
 * the delegating convenience methods) instead of branching on {@link MessageFormat}.
 */
@Component
public class CodecRegistry {

    private final Map<MessageFormat, SchemaCodec> codecs = new EnumMap<>(MessageFormat.class);

    public CodecRegistry(List<SchemaCodec> codecList) {
        for (SchemaCodec c : codecList) {
            if (codecs.put(c.format(), c) != null) {
                throw new IllegalStateException("Duplicate codec for format " + c.format());
            }
        }
    }

    public SchemaCodec codec(MessageFormat format) {
        SchemaCodec c = codecs.get(format);
        if (c == null) {
            throw new IllegalArgumentException("No codec registered for format " + format);
        }
        return c;
    }

    public Object encode(MessageFormat format, Object schema, String json) {
        return codec(format).encode(schema, json);
    }

    public String decode(MessageFormat format, Object schema, Object value) {
        return codec(format).decode(schema, value);
    }

    public String generateExample(MessageFormat format, Object schema) {
        return codec(format).generateExample(schema);
    }
}
