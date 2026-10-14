package com.acme.kafkatester.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.catalog.avro.AvroCatalogProvider;
import com.acme.kafkatester.catalog.protobuf.ProtobufCatalogProvider;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * ADR-007: the consumer decodes by the writer schema id, so a record whose subject is NOT
 * {@code <topic>-value} still decodes with the Gateway's deserializer config (MockSchemaRegistry via
 * {@code mock://}; no Docker).
 */
class DeserializerWriterSchemaTest {

    @Test
    void recordWhoseSubjectDoesNotMatchTopicValueStillDecodes() {
        String url = "mock://writer-schema-" + UUID.randomUUID();
        AvroCatalogProvider avro = new AvroCatalogProvider();
        ProtobufCatalogProvider proto = new ProtobufCatalogProvider();
        LocalSchemas local = new LocalSchemas(new SchemaCatalog(List.of(avro, proto)), avro, proto);
        SchemaRegistryGateway gateway = new SchemaRegistryGateway(mock(SchemaRegistryClient.class),
                SubjectNamingStrategy.topicValue(), local);

        // A foreign producer writes under a record-name subject (not "orders-value") and auto-registers.
        Map<String, Object> producerCfg = new HashMap<>();
        producerCfg.put("schema.registry.url", url);
        producerCfg.put("auto.register.schemas", true);
        producerCfg.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        org.apache.avro.Schema schema = avro.schema("PingEvent").orElseThrow();
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", "1");
        record.put("message", "hello");
        record.put("sequence", 5L);
        record.put("sentAt", 6L);
        record.put("note", null);
        byte[] bytes;
        try (KafkaAvroSerializer serializer = new KafkaAvroSerializer()) {
            serializer.configure(producerCfg, false);
            bytes = serializer.serialize("orders", record);
        }

        // The tool's consumer config: hostile operator values are overridden by the safe deserializer defaults.
        Map<String, Object> base = new HashMap<>();
        base.put("schema.registry.url", url);
        base.put("use.latest.version", true);
        base.put("auto.register.schemas", true);
        Map<String, Object> consumerCfg = gateway.deserializerConfig(base);
        assertThat(consumerCfg).containsEntry("use.latest.version", false).containsEntry("auto.register.schemas", false);
        try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer()) {
            deserializer.configure(consumerCfg, false);
            Object decoded = deserializer.deserialize("orders", bytes);

            assertThat(decoded).isInstanceOf(GenericRecord.class);
            assertThat(((GenericRecord) decoded).get("message").toString()).isEqualTo("hello");
        }
    }
}
