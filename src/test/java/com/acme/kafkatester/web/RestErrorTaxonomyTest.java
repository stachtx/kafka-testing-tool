package com.acme.kafkatester.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.codec.InvalidProtobufPayloadException;
import com.acme.kafkatester.consumer.ConsumerLimitReachedException;
import com.acme.kafkatester.consumer.ConsumerSessionManager;
import com.acme.kafkatester.consumer.ConsumerStartException;
import com.acme.kafkatester.producer.KafkaProducerService;
import com.acme.kafkatester.producer.KafkaSendFailedException;
import com.acme.kafkatester.registry.SchemaRegistrationService;
import com.acme.kafkatester.registry.SchemaRegistryException;
import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;
import com.acme.kafkatester.web.rest.ConsumersController;
import com.acme.kafkatester.web.rest.EventsController;
import com.acme.kafkatester.web.rest.GlobalExceptionHandler;
import com.acme.kafkatester.web.rest.HealthController;
import com.acme.kafkatester.web.rest.ProducerController;
import com.acme.kafkatester.web.rest.SchemaRegistrationController;

/** Gap-closing tests for the error taxonomy codes not covered by {@link RestLayerTest}. */
@WebMvcTest(controllers = { EventsController.class, ProducerController.class, ConsumersController.class,
        SchemaRegistrationController.class, HealthController.class })
@org.springframework.context.annotation.Import({ GlobalExceptionHandler.class, CorrelationIdFilter.class })
class RestErrorTaxonomyTest {

    private static final String VALID_BODY =
            "{\"topic\":\"t\",\"eventType\":\"PingEvent\",\"payload\":{\"id\":\"1\"}}";

    @Autowired MockMvc mvc;

    @MockitoBean SchemaCatalog catalog;
    @MockitoBean KafkaProducerService producer;
    @MockitoBean ConsumerSessionManager consumers;
    @MockitoBean SchemaRegistryGateway gateway;
    @MockitoBean SchemaRegistrationService registration;
    @MockitoBean KafkaSecurityConfig securityConfig;

    private MvcResult postJson(String url, String body, int expectedStatus, String expectedCode) throws Exception {
        return mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(expectedStatus))
                .andExpect(jsonPath("$.errorCode").value(expectedCode))
                .andExpect(jsonPath("$.correlationId").isNotEmpty())
                .andReturn();
    }

    @Test
    void invalidProtobufPayloadIs422() throws Exception {
        when(producer.validate(any())).thenThrow(new InvalidProtobufPayloadException("unknown field nope", null));

        postJson("/api/producer/validate", VALID_BODY, 422, "INVALID_PROTOBUF_PAYLOAD");
    }

    @Test
    void serializationFailureIs422WithoutLeakingCause() throws Exception {
        when(producer.send(any())).thenThrow(
                new SerializationException("Error serializing Avro message", new RuntimeException("password=hunter2")));

        MvcResult r = postJson("/api/producer/send", VALID_BODY, 422, "SERIALIZATION_FAILED");

        assertThat(r.getResponse().getContentAsString()).doesNotContain("hunter2").doesNotContain("stackTrace");
    }

    @Test
    void kafkaSendFailureIs502WithFixedMessageAndNoCause() throws Exception {
        when(producer.send(any())).thenThrow(new KafkaSendFailedException(
                "Send to topic t failed: connect to broker-1.internal:9092 token=abc123",
                new RuntimeException("sasl.jaas.config=secret")));

        MvcResult r = postJson("/api/producer/send", VALID_BODY, 502, "KAFKA_SEND_FAILED");

        assertThat(r.getResponse().getContentAsString())
                .doesNotContain("broker-1.internal").doesNotContain("abc123").doesNotContain("jaas");
    }

    @Test
    void consumerStartFailureIs502AndDoesNotEchoCause() throws Exception {
        when(consumers.startSession(any(), any(), any())).thenThrow(new ConsumerStartException(
                "Could not start consumer session on topic 't'", new RuntimeException("ssl.key.password=hunter2")));

        MvcResult r = postJson("/api/consumers", "{\"topic\":\"t\",\"format\":\"AVRO\",\"offsetMode\":\"LATEST\"}",
                502, "CONSUMER_START_FAILED");

        assertThat(r.getResponse().getContentAsString()).doesNotContain("hunter2");
    }

    @Test
    void schemaRegistryErrorMapsTo502And409AndMasksCredentials() throws Exception {
        when(registration.register(any(), any(), any()))
                .thenThrow(new SchemaRegistryException(
                        "Schema Registry register failed: https://user:pw0rd@registry:8081 basic.auth.user.info=user:pw0rd",
                        new RuntimeException("cause"), 500))
                .thenThrow(new SchemaRegistryException("Schema Registry register failed: incompatible",
                        new RuntimeException("cause"), 409));
        String body = "{\"eventType\":\"PingEvent\",\"format\":\"AVRO\",\"topic\":\"t\"}";

        MvcResult first = postJson("/api/schemas/register", body, 502, "SCHEMA_REGISTRY_ERROR");
        postJson("/api/schemas/register", body, 409, "SCHEMA_REGISTRY_ERROR");

        assertThat(first.getResponse().getContentAsString()).doesNotContain("pw0rd");
    }

    @Test
    void invalidTopicOrMissingFieldsAreValidationErrorsAndNeverReachProducer() throws Exception {
        MvcResult badTopic = postJson("/api/producer/send",
                "{\"topic\":\"has space/\",\"eventType\":\"PingEvent\",\"payload\":{}}", 400, "VALIDATION_ERROR");
        assertThat(badTopic.getResponse().getContentAsString()).contains("\"field\":\"topic\"");

        MvcResult missing = postJson("/api/producer/send", "{\"topic\":\"t\",\"payload\":{}}", 400, "VALIDATION_ERROR");
        assertThat(missing.getResponse().getContentAsString()).contains("eventType");

        postJson("/api/producer/validate", "{\"topic\":\"..\",\"eventType\":\"PingEvent\",\"payload\":{}}",
                400, "VALIDATION_ERROR");
        verifyNoInteractions(producer);
    }

    @Test
    void consumerLimitReachedIs429WithTaxonomyCode() throws Exception {
        when(consumers.startSession(any(), any(), any())).thenThrow(new ConsumerLimitReachedException(10));

        MvcResult r = postJson("/api/consumers", "{\"topic\":\"t\",\"format\":\"AVRO\",\"offsetMode\":\"LATEST\"}",
                429, "CONSUMER_LIMIT_REACHED");

        assertThat(r.getResponse().getContentAsString()).contains("10");
    }

    @Test
    void unexpectedIllegalArgumentIsInternalErrorWithoutEchoingTheMessage() throws Exception {
        when(producer.send(any())).thenThrow(new IllegalArgumentException("No producer configured for secret.internal.Thing"));

        MvcResult r = postJson("/api/producer/send", VALID_BODY, 500, "INTERNAL_ERROR");

        assertThat(r.getResponse().getContentAsString()).doesNotContain("secret.internal").doesNotContain("No producer");
    }

    @Test
    void unsupportedFormatAndBlankTopicStayValidationErrors() throws Exception {
        postJson("/api/consumers", "{\"topic\":\"t\",\"format\":\"THRIFT\",\"offsetMode\":\"LATEST\"}",
                400, "VALIDATION_ERROR");
        postJson("/api/producer/send", "{\"topic\":\"  \",\"eventType\":\"PingEvent\",\"payload\":{}}",
                400, "VALIDATION_ERROR");
        postJson("/api/producer/send",
                "{\"topic\":\"t\",\"eventType\":\"PingEvent\",\"format\":\"bogus\",\"payload\":{}}",
                400, "VALIDATION_ERROR");
        verifyNoInteractions(producer, consumers);
    }

    @Test
    void healthReportsConfiguredEarliestCap() throws Exception {
        when(securityConfig.producerProperties()).thenThrow(new IllegalStateException("x"));
        when(gateway.client()).thenThrow(new IllegalStateException("x"));

        mvc.perform(get("/api/kafka/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.earliestCap").value(1000));
    }

    @Test
    void healthEndpointNeverThrowsWhenEverythingIsBroken() throws Exception {
        when(securityConfig.producerProperties()).thenThrow(new IllegalStateException("password=hunter2"));
        when(gateway.client()).thenThrow(new IllegalStateException("http://u:hunter2@registry"));

        MvcResult r = mvc.perform(get("/api/kafka/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DOWN"))
                .andExpect(jsonPath("$.kafka.reachable").value(false))
                .andExpect(jsonPath("$.schemaRegistry.reachable").value(false))
                .andReturn();

        assertThat(r.getResponse().getContentAsString()).doesNotContain("hunter2");
    }
}
