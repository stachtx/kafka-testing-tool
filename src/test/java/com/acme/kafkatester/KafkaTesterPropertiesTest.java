package com.acme.kafkatester;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import jakarta.validation.Validation;

/** Defaults and bean-validation of the {@code kafkatester.consumer.*} limits. */
class KafkaTesterPropertiesTest {

    private static int violations(KafkaTesterProperties p) {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            return factory.getValidator().validate(p).size();
        }
    }

    @Test
    void defaultsAreSaneAndValid() {
        KafkaTesterProperties p = new KafkaTesterProperties();
        assertThat(p.getConsumer().getMaxSessions()).isEqualTo(10);
        assertThat(p.getConsumer().getIdleTtl()).isEqualTo(Duration.ofMinutes(30));
        assertThat(p.getConsumer().getMaxPayloadBytes()).isEqualTo(64 * 1024);
        assertThat(p.getConsumer().getEarliestCap()).isEqualTo(1000);
        assertThat(violations(p)).isZero();
    }

    @Test
    void nonPositiveLimitsAreRejected() {
        KafkaTesterProperties p = new KafkaTesterProperties();
        p.getConsumer().setEarliestCap(0);
        p.getConsumer().setMaxSessions(0);
        p.getConsumer().setMaxPayloadBytes(-1);
        p.getConsumer().setIdleTtl(null);
        assertThat(violations(p)).isEqualTo(4);
    }
}
