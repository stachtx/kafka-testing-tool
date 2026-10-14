package com.acme.kafkatester.consumer;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import com.acme.kafkatester.codec.MessageFormat;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

/**
 * Creates a configured, per-session Confluent value deserializer. Avro yields {@code GenericRecord};
 * Protobuf yields {@code DynamicMessage} (derive.type / specific.protobuf.value.type left unset).
 */
@Component
class ValueDeserializerFactory {

    private final Map<MessageFormat, Supplier<Deserializer<Object>>> suppliers = new EnumMap<>(MessageFormat.class);

    @SuppressWarnings({ "unchecked", "rawtypes" })
    ValueDeserializerFactory() {
        suppliers.put(MessageFormat.AVRO, () -> (Deserializer) new KafkaAvroDeserializer());
        suppliers.put(MessageFormat.PROTOBUF, () -> (Deserializer) new KafkaProtobufDeserializer<>());
    }

    Deserializer<Object> create(MessageFormat format, Map<String, ?> config) {
        Deserializer<Object> d = suppliers.get(format).get();
        d.configure(config, false);
        return d;
    }
}
