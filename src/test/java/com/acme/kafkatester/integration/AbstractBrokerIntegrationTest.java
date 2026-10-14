package com.acme.kafkatester.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import com.acme.kafkatester.producer.KafkaProducerService;
import com.acme.kafkatester.producer.KafkaProducerService.ProducerRequest;
import com.acme.kafkatester.producer.KafkaProducerService.ProducerResult;

/**
 * Base for the Testcontainers integration tests (Group 12). Tagged {@code integration}, so the default
 * {@code test} task skips them; run with {@code ./gradlew integrationTest}. When no Docker daemon is
 * reachable they are skipped (not failed) by {@code @EnabledIfDockerAvailable}, which must be placed on
 * each concrete subclass (the Testcontainers condition does not look at superclasses). The broker and registry
 * are started lazily when the Spring context asks for the dynamic properties, i.e. only if the
 * tests actually run.
 */
@Tag("integration")
@SpringBootTest
@AutoConfigureMockMvc
abstract class AbstractBrokerIntegrationTest {

    static final Duration TIMEOUT = Duration.ofSeconds(60);

    @Autowired protected MockMvc mvc;
    @Autowired protected KafkaProducerService producer;

    @DynamicPropertySource
    static void infraProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KafkaRegistryInfra::bootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", KafkaRegistryInfra::registryUrl);
    }

    // ---- helpers -----------------------------------------------------------------------------

    protected static String uniqueTopic(String prefix) {
        return "it-" + prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    protected static void createTopic(String topic) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaRegistryInfra.bootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    /** Registers the PingEvent schema through the real REST endpoint (expects HTTP 200). */
    protected void registerViaApi(String format, String topic) throws Exception {
        mvc.perform(post("/api/schemas/register").contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"PingEvent\",\"format\":\"" + format + "\",\"topic\":\"" + topic + "\"}"))
                .andExpect(status().isOk());
    }

    protected ProducerResult send(String topic, String format, String key, String payload) {
        return producer.send(new ProducerRequest(topic, "PingEvent", format, key, Map.of("source", "it"), null,
                payload));
    }

    /** Number of versions of {@code subject} according to the registry's own REST API (independent of the app). */
    protected static int registryVersionCount(String subject) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(KafkaRegistryInfra.registryUrl() + "/subjects/" + subject + "/versions"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) {
            return 0;
        }
        assertThat(response.statusCode()).isEqualTo(200);
        Matcher m = Pattern.compile("\\d+").matcher(response.body());
        int count = 0;
        while (m.find()) {
            count++;
        }
        return count;
    }

    protected static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Timed out after " + TIMEOUT + " waiting for: " + what);
    }
}
