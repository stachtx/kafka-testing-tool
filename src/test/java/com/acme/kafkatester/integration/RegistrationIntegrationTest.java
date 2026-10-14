package com.acme.kafkatester.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;

/**
 * Registration safety against a real Schema Registry: the preview endpoint is read-only (the
 * subject's version count never changes) and {@code POST /api/schemas/register} writes exactly once.
 */
@EnabledIfDockerAvailable
class RegistrationIntegrationTest extends AbstractBrokerIntegrationTest {

    @Test
    void previewIsReadOnlyAndRegisterAddsExactlyOneVersion() throws Exception {
        String topic = uniqueTopic("registration");
        String subject = topic + "-value";
        assertThat(registryVersionCount(subject)).isZero();

        // preview of an unknown subject: first registration, and nothing is written
        for (int i = 0; i < 3; i++) {
            mvc.perform(get("/api/schemas/PingEvent/registration-preview")
                    .param("format", "AVRO").param("topic", topic))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.subject").value(subject))
                    .andExpect(jsonPath("$.firstRegistration").value(true))
                    .andExpect(jsonPath("$.compatible").value(true));
        }
        assertThat(registryVersionCount(subject)).as("preview must not create the subject").isZero();

        // the one and only write
        mvc.perform(post("/api/schemas/register").contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"PingEvent\",\"format\":\"AVRO\",\"topic\":\"" + topic + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subject").value(subject))
                .andExpect(jsonPath("$.version").value(1));
        assertThat(registryVersionCount(subject)).isEqualTo(1);

        // preview of the registered subject: sees the registry version, still no write
        for (int i = 0; i < 3; i++) {
            mvc.perform(get("/api/schemas/PingEvent/registration-preview")
                    .param("format", "AVRO").param("topic", topic))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.firstRegistration").value(false))
                    .andExpect(jsonPath("$.registryVersion").value(1))
                    .andExpect(jsonPath("$.identical").value(true));
        }
        assertThat(registryVersionCount(subject)).as("preview must not add versions").isEqualTo(1);

        // registering the identical schema again is idempotent: still one version
        mvc.perform(post("/api/schemas/register").contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"PingEvent\",\"format\":\"AVRO\",\"topic\":\"" + topic + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(1));
        assertThat(registryVersionCount(subject)).isEqualTo(1);
    }

    @Test
    void registeringChangedCompatibleSchemaAddsVersionTwo() throws Exception {
        String topic = uniqueTopic("registration-v2");
        String subject = topic + "-value";

        // v1 written straight to the registry: PingEvent without the optional "note" field
        String v1 = "{\"type\":\"record\",\"name\":\"PingEvent\",\"namespace\":\"com.acme.kafkatester.sample\","
                + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"message\",\"type\":\"string\"},"
                + "{\"name\":\"sequence\",\"type\":\"long\"},{\"name\":\"sentAt\",\"type\":\"long\"}]}";
        String body = "{\"schema\":\"" + v1.replace("\"", "\\\"") + "\"}";
        HttpResponse<String> created = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(KafkaRegistryInfra.registryUrl() + "/subjects/" + subject + "/versions"))
                        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(created.statusCode()).isEqualTo(200);
        assertThat(registryVersionCount(subject)).isEqualTo(1);

        // the local schema differs (adds "note" with a default): preview sees v1, compatible, not identical, no write
        mvc.perform(get("/api/schemas/PingEvent/registration-preview")
                .param("format", "AVRO").param("topic", topic))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.firstRegistration").value(false))
                .andExpect(jsonPath("$.registryVersion").value(1))
                .andExpect(jsonPath("$.compatible").value(true))
                .andExpect(jsonPath("$.identical").value(false));
        assertThat(registryVersionCount(subject)).as("preview must not add versions").isEqualTo(1);

        mvc.perform(post("/api/schemas/register").contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"PingEvent\",\"format\":\"AVRO\",\"topic\":\"" + topic + "\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(2));
        assertThat(registryVersionCount(subject)).isEqualTo(2);
    }
}
