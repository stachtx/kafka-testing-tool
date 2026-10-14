package com.acme.kafkatester;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;

/** The "local" profile points the app at the docker-compose.yml stack (Kafka :9092, Schema Registry :8081). */
@SpringBootTest
@ActiveProfiles("local")
class LocalProfileTest {

    @Autowired Environment env;

    @Test
    void localProfileTargetsDockerComposeStack() {
        assertThat(env.getProperty("spring.kafka.bootstrap-servers")).isEqualTo("localhost:9092");
        assertThat(env.getProperty("spring.kafka.properties.schema.registry.url")).isEqualTo("http://localhost:8081");
    }
}
