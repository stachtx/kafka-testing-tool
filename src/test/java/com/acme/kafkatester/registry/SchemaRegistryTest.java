package com.acme.kafkatester.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.catalog.avro.AvroCatalogProvider;
import com.acme.kafkatester.catalog.protobuf.ProtobufCatalogProvider;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

class SchemaRegistryTest {

    private static final String TOPIC_NAME_STRATEGY = SchemaRegistryGateway.TOPIC_NAME_STRATEGY;

    private SchemaRegistryClient client;
    private LocalSchemas local;
    private SchemaRegistryGateway gateway;
    private SchemaRegistrationService service;

    @BeforeEach
    void setUp() {
        client = mock(SchemaRegistryClient.class);
        AvroCatalogProvider avro = new AvroCatalogProvider();
        ProtobufCatalogProvider proto = new ProtobufCatalogProvider();
        local = new LocalSchemas(new SchemaCatalog(List.of(avro, proto)), avro, proto);
        SubjectNamingStrategy naming = SubjectNamingStrategy.topicValue();
        gateway = new SchemaRegistryGateway(client, naming, local);
        service = new SchemaRegistrationService(client, local, naming);
    }

    private long registerCalls() {
        return mockingDetails(client).getInvocations().stream()
                .filter(i -> i.getMethod().getName().equals("register")).count();
    }

    @Test
    void serializerSafeDefaultsAreForcedOverHostileBaseConfig() {
        Map<String, Object> cfg = gateway.serializerConfig(Map.of("auto.register.schemas", true,
                "use.latest.version", false, "value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy", "x", "y"));
        assertThat(cfg).containsEntry("auto.register.schemas", false)
                .containsEntry("use.latest.version", true)
                .containsEntry("latest.compatibility.strict", true)
                .containsEntry("latest.cache.ttl.sec", SchemaRegistryGateway.LATEST_CACHE_TTL_SEC)
                .containsEntry("value.subject.name.strategy", TOPIC_NAME_STRATEGY)
                .containsEntry("x", "y");
        // an operator-supplied "never expire" TTL is overridden too
        assertThat(gateway.serializerConfig(Map.of("latest.cache.ttl.sec", -1)))
                .containsEntry("latest.cache.ttl.sec", SchemaRegistryGateway.LATEST_CACHE_TTL_SEC);
        assertThat(gateway.serializerConfig(null)).containsAllEntriesOf(SchemaRegistryGateway.serializerSafeDefaults());
    }

    @Test
    void deserializerForcesWriterSchemaDecodingAndNeverAutoRegisters() {
        Map<String, Object> cfg = gateway.deserializerConfig(Map.of("auto.register.schemas", true,
                "use.latest.version", true, "value.subject.name.strategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy"));
        assertThat(cfg).containsEntry("auto.register.schemas", false)
                .containsEntry("use.latest.version", false)
                .containsEntry("value.subject.name.strategy", TOPIC_NAME_STRATEGY);
        assertThat(gateway.deserializerConfig(null)).containsAllEntriesOf(SchemaRegistryGateway.deserializerSafeDefaults());
    }

    @Test
    void serializerPicksUpNewlyRegisteredLatestVersionAfterCacheTtl() throws Exception {
        String scope = "latest-ttl-test";
        var registry = io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry.getClientForScope(scope);
        org.apache.avro.Schema v1 = new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"E\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
        org.apache.avro.Schema v2 = new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"E\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},"
                        + "{\"name\":\"note\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
        int id1 = registry.register("t-value", new io.confluent.kafka.schemaregistry.avro.AvroSchema(v1));

        Map<String, Object> cfg = new java.util.HashMap<>(
                gateway.serializerConfig(Map.of("schema.registry.url", "mock://" + scope)));
        cfg.put("latest.cache.ttl.sec", 2); // shortened for the test; the production value is asserted above
        try (var serializer = new io.confluent.kafka.serializers.KafkaAvroSerializer()) {
            serializer.configure(cfg, false);
            org.apache.avro.generic.GenericRecord record = new org.apache.avro.generic.GenericData.Record(v2);
            record.put("id", "a");
            assertThat(schemaIdOf(serializer.serialize("t", record))).isEqualTo(id1);

            int id2 = registry.register("t-value", new io.confluent.kafka.schemaregistry.avro.AvroSchema(v2));
            assertThat(id2).isNotEqualTo(id1);
            Thread.sleep(2500);

            assertThat(schemaIdOf(serializer.serialize("t", record))).isEqualTo(id2);
        } finally {
            io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry.dropScope(scope);
        }
    }

    private static int schemaIdOf(byte[] serialized) {
        return java.nio.ByteBuffer.wrap(serialized, 1, 4).getInt();
    }

    @Test
    void subjectStrategyMatchesSubjectNamingStrategy() {
        assertThat(TOPIC_NAME_STRATEGY).isEqualTo(
                "io.confluent.kafka.serializers.subject.TopicNameStrategy");
        assertThat(SubjectNamingStrategy.topicValue().subjectFor("orders", "PingEvent")).isEqualTo("orders-value");
    }

    @Test
    void privateAvroMetadataIsStrippedFromTheParsedSchema() throws Exception {
        org.apache.avro.Schema withPrivate = new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"X\",\"defaultTopic\":\"t\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}");

        org.apache.avro.Schema stripped = LocalSchemas.stripPrivateAvroProps(withPrivate);

        assertThat(withPrivate.getObjectProp("defaultTopic")).isEqualTo("t");
        assertThat(stripped.getObjectProp("defaultTopic")).isNull();
        assertThat(stripped.getFields()).extracting(org.apache.avro.Schema.Field::name).containsExactly("a");
        // shipped schemas carry no private metadata and no descriptor.proto dependency
        assertThat(local.parsed(local.describe("PingEvent", "AVRO")).canonicalString()).doesNotContain("defaultTopic");
        assertThat(local.parsed(local.describe("PingEvent", "PROTOBUF")).canonicalString())
                .doesNotContain("default_topic").doesNotContain("descriptor.proto");
    }

    @Test
    void guardPassesWhenLocalEqualsRegistryLatest() throws Exception {
        ParsedSchema localSchema = local.parsed(local.describe("PingEvent", "AVRO"));
        when(client.getLatestSchemaMetadata("t-value")).thenReturn(new SchemaMetadata(7, 3, "AVRO", List.of(), "{}"));
        when(client.getSchemaBySubjectAndId("t-value", 7)).thenReturn(localSchema);

        gateway.assertLocalMatchesRegistryLatest("PingEvent", "AVRO", "t");

        assertThat(registerCalls()).isZero();
    }

    @Test
    void guardRejectsDivergingSchemaWith409() throws Exception {
        ParsedSchema other = new io.confluent.kafka.schemaregistry.avro.AvroSchema(new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"PingEvent\",\"namespace\":\"com.acme.kafkatester.sample\","
                        + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"));
        when(client.getLatestSchemaMetadata("t-value")).thenReturn(new SchemaMetadata(7, 3, "AVRO", List.of(), "{}"));
        when(client.getSchemaBySubjectAndId("t-value", 7)).thenReturn(other);

        assertThatThrownBy(() -> gateway.assertLocalMatchesRegistryLatest("PingEvent", "AVRO", "t"))
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessageContaining("differs from registry latest (v3)")
                .hasMessageContaining("Event Catalog")
                .extracting(e -> ((SchemaRegistryException) e).httpStatus()).isEqualTo(409);
    }

    @Test
    void guardRejectsMissingSubjectWith409() throws Exception {
        when(client.getLatestSchemaMetadata("t-value")).thenThrow(new RestClientException("nf", 404, 40401));

        assertThatThrownBy(() -> gateway.assertLocalMatchesRegistryLatest("PingEvent", "PROTOBUF", "t"))
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessageContaining("no schema registered for subject t-value; register it first")
                .extracting(e -> ((SchemaRegistryException) e).httpStatus()).isEqualTo(409);
    }

    @Test
    void guardSurfacesUnreachableRegistryAsRegistryException() throws Exception {
        when(client.getLatestSchemaMetadata("t-value")).thenThrow(new java.io.IOException("connection refused"));

        assertThatThrownBy(() -> gateway.assertLocalMatchesRegistryLatest("PingEvent", "AVRO", "t"))
                .isInstanceOf(SchemaRegistryException.class)
                .extracting(e -> ((SchemaRegistryException) e).httpStatus()).isEqualTo(-1);
    }

    @Test
    void previewOfExistingSubjectReturnsBothSchemasAndNeverRegisters() throws Exception {
        ParsedSchema localSchema = local.parsed(local.describe("PingEvent", "AVRO"));
        when(client.getCompatibility("t-value")).thenReturn("BACKWARD");
        when(client.getLatestSchemaMetadata("t-value")).thenReturn(new SchemaMetadata(7, 3, "AVRO", List.of(), "{}"));
        when(client.getSchemaBySubjectAndId("t-value", 7)).thenReturn(localSchema);
        when(client.testCompatibilityVerbose(anyString(), any(ParsedSchema.class))).thenReturn(List.of());

        RegistrationPreview p = gateway.preview("PingEvent", "AVRO", "t");

        assertThat(p.subject()).isEqualTo("t-value");
        assertThat(p.firstRegistration()).isFalse();
        assertThat(p.registryVersion()).isEqualTo(3);
        assertThat(p.compatibilityMode()).isEqualTo("BACKWARD");
        assertThat(p.compatible()).isTrue();
        assertThat(p.identical()).isTrue();
        assertThat(p.localSchema()).contains("PingEvent");
        assertThat(p.registrySchema()).isNotBlank();
        assertThat(registerCalls()).isZero();
    }

    @Test
    void previewReportsIncompatibilityMessages() throws Exception {
        ParsedSchema localSchema = local.parsed(local.describe("PingEvent", "AVRO"));
        when(client.getCompatibility("t-value")).thenReturn("FULL");
        when(client.getLatestSchemaMetadata("t-value")).thenReturn(new SchemaMetadata(1, 1, "AVRO", List.of(), "{}"));
        when(client.getSchemaBySubjectAndId("t-value", 1)).thenReturn(localSchema);
        when(client.testCompatibilityVerbose(anyString(), any(ParsedSchema.class))).thenReturn(List.of("field removed"));

        RegistrationPreview p = gateway.preview("PingEvent", "AVRO", "t");

        assertThat(p.compatible()).isFalse();
        assertThat(p.compatibilityMessages()).containsExactly("field removed");
        assertThat(registerCalls()).isZero();
    }

    @Test
    void previewOfMissingSubjectIsFirstRegistrationUsingDefaultTopicAndGlobalMode() throws Exception {
        when(client.getCompatibility(anyString())).thenThrow(new RestClientException("nf", 404, 40408));
        when(client.getCompatibility(null)).thenReturn("BACKWARD");
        when(client.getLatestSchemaMetadata(anyString())).thenThrow(new RestClientException("nf", 404, 40401));

        RegistrationPreview p = gateway.preview("PingEvent", "PROTOBUF", null);

        assertThat(p.firstRegistration()).isTrue();
        assertThat(p.subject()).isEqualTo(p.topic() + "-value");
        assertThat(p.registryVersion()).isNull();
        assertThat(p.compatibilityMode()).isEqualTo("BACKWARD");
        assertThat(p.localSchema()).contains("PingEvent");
        assertThat(registerCalls()).isZero();
    }

    @Test
    void registerIsTheOnlyPathThatCallsRegisterApi() throws Exception {
        when(client.register(anyString(), any(ParsedSchema.class))).thenReturn(42);
        when(client.getVersion(anyString(), any(ParsedSchema.class))).thenReturn(5);

        var result = service.register("PingEvent", "AVRO", "t");

        assertThat(result.subject()).isEqualTo("t-value");
        assertThat(result.schemaId()).isEqualTo(42);
        assertThat(result.version()).isEqualTo(5);
        assertThat(registerCalls()).isEqualTo(1);
        assertThat(Arrays.stream(SchemaRegistryGateway.class.getDeclaredMethods())
                .map(m -> m.getName())).noneMatch(n -> n.toLowerCase().contains("register"));
    }

    @Test
    void registryFailureMapsToSanitizedException() throws Exception {
        when(client.register(anyString(), any(ParsedSchema.class))).thenThrow(
                new RestClientException("Unauthorized calling https://svc:s3cret@registry.internal:8081/subjects", 401, 40101));

        assertThatThrownBy(() -> service.register("PingEvent", "AVRO", "t"))
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessageNotContaining("s3cret")
                .hasMessageContaining("registry.internal")
                .extracting(e -> ((SchemaRegistryException) e).httpStatus()).isEqualTo(401);
    }

    @Test
    void gatewayReadFailureMapsToSchemaRegistryException() throws Exception {
        when(client.getSchemaById(9)).thenThrow(new RestClientException("boom password=hunter2", 500, 50001));

        assertThatThrownBy(() -> gateway.schemaById(9))
                .isInstanceOf(SchemaRegistryException.class)
                .hasMessageNotContaining("hunter2");
    }
}
