package com.acme.kafkatester.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.acme.kafkatester.catalog.EventTypeDescriptor;
import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.catalog.UnknownEventTypeException;
import com.acme.kafkatester.codec.InvalidAvroPayloadException;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.consumer.ConsumerSession.Info;
import com.acme.kafkatester.consumer.ConsumerSession.OffsetMode;
import com.acme.kafkatester.consumer.ConsumerSession.Status;
import com.acme.kafkatester.consumer.ConsumerSessionManager;
import com.acme.kafkatester.consumer.ConsumerSessionNotFoundException;
import com.acme.kafkatester.producer.KafkaProducerService;
import com.acme.kafkatester.registry.SchemaRegistrationService;
import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;
import com.acme.kafkatester.web.rest.ConsumersController;
import com.acme.kafkatester.web.rest.EventsController;
import com.acme.kafkatester.web.rest.GlobalExceptionHandler;
import com.acme.kafkatester.web.rest.HealthController;
import com.acme.kafkatester.web.rest.ProducerController;
import com.acme.kafkatester.web.rest.SchemaRegistrationController;

@WebMvcTest(controllers = { EventsController.class, ProducerController.class, ConsumersController.class,
        SchemaRegistrationController.class, HealthController.class })
@org.springframework.context.annotation.Import({ GlobalExceptionHandler.class, CorrelationIdFilter.class })
class RestLayerTest {

    @Autowired MockMvc mvc;

    @MockitoBean SchemaCatalog catalog;
    @MockitoBean KafkaProducerService producer;
    @MockitoBean ConsumerSessionManager consumers;
    @MockitoBean SchemaRegistryGateway gateway;
    @MockitoBean SchemaRegistrationService registration;
    @MockitoBean KafkaSecurityConfig securityConfig;

    private static EventTypeDescriptor ping(String format) {
        return new EventTypeDescriptor("PingEvent", format,
                List.of(new EventTypeDescriptor.Field("id", "string", false)), "sandbox.ping", "A ping");
    }

    @Test
    void listsEventTypesPaginated() throws Exception {
        when(catalog.list(0, 1)).thenReturn(new SchemaCatalog.Page<>(List.of(ping("AVRO")), 0, 1, 2));

        mvc.perform(get("/api/events").param("offset", "0").param("limit", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].name").value("PingEvent"))
                .andExpect(jsonPath("$.items[0].format").value("AVRO"))
                .andExpect(jsonPath("$.total").value(2))
                .andExpect(jsonPath("$.limit").value(1));
    }

    @Test
    void unknownEventTypeIsProblemDetailWithCorrelationId() throws Exception {
        when(catalog.resolve("Nope")).thenThrow(new UnknownEventTypeException("Nope"));

        MvcResult result = mvc.perform(get("/api/events/Nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("UNKNOWN_EVENT_TYPE"))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.correlationId").isNotEmpty())
                .andExpect(header().exists(CorrelationIdFilter.HEADER))
                .andReturn();
        String body = result.getResponse().getContentAsString();
        org.assertj.core.api.Assertions.assertThat(body).doesNotContain("at com.acme").doesNotContain("stackTrace");
    }

    @Test
    void invalidJsonBodyIsInvalidJson400() throws Exception {
        mvc.perform(post("/api/producer/send").contentType(MediaType.APPLICATION_JSON).content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_JSON"))
                .andExpect(jsonPath("$.correlationId").isNotEmpty());
    }

    @Test
    void payloadNotMatchingSchemaMapsToTaxonomyCode() throws Exception {
        when(producer.validate(any())).thenThrow(new InvalidAvroPayloadException("field id is required", null));

        mvc.perform(post("/api/producer/validate").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"t\",\"eventType\":\"PingEvent\",\"payload\":{}}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.errorCode").value("INVALID_AVRO_PAYLOAD"));
    }

    @Test
    void listsConsumerSessions() throws Exception {
        Info info = new Info("s1", "kafka-tester-s1", "t", MessageFormat.AVRO, OffsetMode.LATEST,
                Instant.parse("2026-01-01T00:00:00Z"), 0, Status.LISTENING, null);
        when(consumers.listSessions()).thenReturn(List.of(info));

        mvc.perform(get("/api/consumers"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value("s1"))
                .andExpect(jsonPath("$[0].format").value("AVRO"))
                .andExpect(jsonPath("$[0].status").value("LISTENING"));
    }

    @Test
    void pollAndStopUnknownSessionAre404() throws Exception {
        when(consumers.poll(eq("ghost"), anyLong())).thenThrow(new ConsumerSessionNotFoundException("ghost"));
        doThrow(new ConsumerSessionNotFoundException("ghost")).when(consumers).stopSession("ghost");

        mvc.perform(get("/api/consumers/ghost/events").param("sinceOffset", "0"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("CONSUMER_SESSION_NOT_FOUND"));
        mvc.perform(delete("/api/consumers/ghost"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("CONSUMER_SESSION_NOT_FOUND"));
    }

    @Test
    void unexpectedErrorsHideDetailAndSecrets() throws Exception {
        when(catalog.list(0, 50)).thenThrow(new IllegalStateException("boom password=hunter2 at internal.Class"));

        MvcResult result = mvc.perform(get("/api/events"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.errorCode").value("INTERNAL_ERROR"))
                .andExpect(jsonPath("$.correlationId").isNotEmpty())
                .andReturn();
        org.assertj.core.api.Assertions.assertThat(result.getResponse().getContentAsString())
                .doesNotContain("hunter2").doesNotContain("internal.Class").doesNotContain("stackTrace");
    }

    @Test
    void badStartRequestIsValidationError() throws Exception {
        mvc.perform(post("/api/consumers").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"topic\":\"bad topic!\",\"format\":\"AVRO\",\"offsetMode\":\"LATEST\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"));
        org.mockito.Mockito.verifyNoInteractions(consumers);
    }
}
