package com.acme.kafkatester.consumer;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import com.acme.kafkatester.codec.CodecRegistry;
import com.acme.kafkatester.codec.MessageFormat;
import com.google.protobuf.Message;

/**
 * Turns a value produced by a Confluent deserializer into JSON via the {@link CodecRegistry}. The
 * schema is taken from the deserialized value itself (which the deserializer resolved from the
 * registry by schema id), never from the local catalog, so topics unknown to the catalog decode too.
 */
@Component
class ValueDecoder {

    private final CodecRegistry codecs;

    ValueDecoder(CodecRegistry codecs) {
        this.codecs = codecs;
    }

    String toJson(MessageFormat format, Object value) {
        Object schema;
        if (value instanceof GenericRecord record) {
            schema = record.getSchema();
        } else if (value instanceof Message message) {
            schema = message.getDescriptorForType();
        } else {
            throw new IllegalArgumentException("Unsupported deserialized value type: "
                    + (value == null ? "null" : value.getClass().getName()));
        }
        return codecs.decode(format, schema, value);
    }
}
