package com.acme.kafkatester.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;

class KafkaSecurityConfigRedactionTest {

    @Test
    void masksEverySecretShapedKeyKeepsOthersAndDoesNotMutateInput() {
        KafkaSecurityConfig config = new KafkaSecurityConfig(new KafkaProperties());
        Map<String, Object> in = new LinkedHashMap<>();
        in.put("bootstrap.servers", "broker:9092");
        in.put("security.protocol", "SASL_SSL");
        in.put("sasl.jaas.config", "PlainLoginModule required password=\"p1\";");
        in.put("ssl.keystore.password", "p2");
        in.put("ssl.truststore.PASSWORD", "p3");
        in.put("basic.auth.user.info", "u:p4");
        in.put("bearer.auth.token", "p5");
        in.put("client.secret", "p6");

        Map<String, Object> out = config.redacted(in);

        assertThat(out).containsEntry("bootstrap.servers", "broker:9092").containsEntry("security.protocol", "SASL_SSL");
        assertThat(out.toString()).doesNotContain("p1\"").doesNotContain("p2").doesNotContain("p3")
                .doesNotContain("p4").doesNotContain("p5").doesNotContain("p6");
        assertThat(out).hasSize(in.size());
        assertThat(in.get("ssl.keystore.password")).isEqualTo("p2");
    }
}
