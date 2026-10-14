package com.acme.kafkatester.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.acme.kafkatester.catalog.avro.AvroCatalogProvider;
import com.acme.kafkatester.catalog.protobuf.ProtobufCatalogProvider;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

class SchemaCodecTest {

    private final CodecRegistry registry =
            new CodecRegistry(List.of(new AvroSchemaCodec(), new ProtobufSchemaCodec()));
    private final Schema avroSchema = new AvroCatalogProvider().schema("PingEvent").orElseThrow();
    private final Descriptor protoDescriptor = new ProtobufCatalogProvider().descriptor("PingEvent").orElseThrow();

    @Test
    void avroJsonToGenericRecordAndBack() {
        String json = "{\"id\":\"a1\",\"message\":\"hi\",\"sequence\":7,\"sentAt\":1700000000000,"
                + "\"note\":{\"string\":\"n\"}}";
        Object encoded = registry.encode(MessageFormat.AVRO, avroSchema, json);
        assertThat(encoded).isInstanceOf(GenericRecord.class);
        assertThat(((GenericRecord) encoded).get("sequence")).isEqualTo(7L);

        String back = registry.decode(MessageFormat.AVRO, avroSchema, encoded);
        assertThat(registry.encode(MessageFormat.AVRO, avroSchema, back)).isEqualTo(encoded);
    }

    @Test
    void protobufJsonToDynamicMessageAndBack() {
        String json = "{\"id\":\"p1\",\"message\":\"hi\",\"sequence\":\"7\",\"sent_at\":\"1700000000000\"}";
        Object encoded = registry.encode(MessageFormat.PROTOBUF, protoDescriptor, json);
        assertThat(encoded).isInstanceOf(DynamicMessage.class);

        String back = registry.decode(MessageFormat.PROTOBUF, protoDescriptor, encoded);
        assertThat(back).contains("\"id\"").contains("p1").contains("sent_at");
        assertThat(registry.encode(MessageFormat.PROTOBUF, protoDescriptor, back)).isEqualTo(encoded);
    }

    @Test
    void invalidAvroPayloadYieldsAvroSpecificException() {
        assertThatThrownBy(() -> registry.encode(MessageFormat.AVRO, avroSchema, "{\"id\":123}"))
                .isInstanceOf(InvalidAvroPayloadException.class)
                .isInstanceOf(InvalidPayloadException.class)
                .hasMessageNotContaining("Exception in thread");
        assertThatThrownBy(() -> registry.encode(MessageFormat.AVRO, avroSchema, "not json"))
                .isInstanceOf(InvalidAvroPayloadException.class);
    }

    @Test
    void invalidProtobufPayloadYieldsProtobufSpecificException() {
        assertThatThrownBy(() -> registry.encode(MessageFormat.PROTOBUF, protoDescriptor, "{\"nope\":1}"))
                .isInstanceOf(InvalidProtobufPayloadException.class);
        assertThatThrownBy(() -> registry.encode(MessageFormat.PROTOBUF, protoDescriptor, "{broken"))
                .isInstanceOf(InvalidProtobufPayloadException.class);
    }

    @Test
    void generatedExamplesRoundTripThroughSameCodec() {
        String avroExample = registry.generateExample(MessageFormat.AVRO, avroSchema);
        Object avro = registry.encode(MessageFormat.AVRO, avroSchema, avroExample);
        assertThat(registry.decode(MessageFormat.AVRO, avroSchema, avro)).isNotBlank();

        String protoExample = registry.generateExample(MessageFormat.PROTOBUF, protoDescriptor);
        Object proto = registry.encode(MessageFormat.PROTOBUF, protoDescriptor, protoExample);
        assertThat(registry.decode(MessageFormat.PROTOBUF, protoDescriptor, proto)).isNotBlank();
    }

    @Test
    void providersGenerateExamplesViaCodec() {
        assertThat(new AvroCatalogProvider().generateExample("PingEvent")).contains("\"id\"");
        assertThat(new ProtobufCatalogProvider().generateExample("PingEvent")).contains("\"id\"");
    }

    @Test
    void registryDispatchesByFormatAndParsesNames() {
        assertThat(registry.codec(MessageFormat.AVRO)).isInstanceOf(AvroSchemaCodec.class);
        assertThat(registry.codec(MessageFormat.PROTOBUF)).isInstanceOf(ProtobufSchemaCodec.class);
        assertThat(MessageFormat.parse("protobuf")).isEqualTo(MessageFormat.PROTOBUF);
        assertThatThrownBy(() -> MessageFormat.parse("thrift")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void wrongSchemaTypeIsRejected() {
        assertThatThrownBy(() -> registry.encode(MessageFormat.AVRO, protoDescriptor, "{}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
