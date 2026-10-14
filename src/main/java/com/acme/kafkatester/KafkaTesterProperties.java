package com.acme.kafkatester;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Application-specific settings bound from the {@code kafkatester.*} namespace. */
@ConfigurationProperties("kafkatester")
@Validated
public class KafkaTesterProperties {

    @Valid
    private final Consumer consumer = new Consumer();

    public Consumer getConsumer() {
        return consumer;
    }

    public static class Consumer {

        /** Maximum records an EARLIEST consumer session reads before it auto-stops. */
        @Min(1)
        private int earliestCap = 1000;

        /** Maximum number of concurrently held consumer sessions; further starts are rejected (429). */
        @Min(1)
        private int maxSessions = 10;

        /** Sessions that were not polled for this long are torn down by a background sweeper. */
        @NotNull
        private Duration idleTtl = Duration.ofMinutes(30);

        /** Decoded payload strings larger than this many UTF-8 bytes are truncated (and flagged). */
        @Min(1)
        private int maxPayloadBytes = 64 * 1024;

        public int getEarliestCap() {
            return earliestCap;
        }

        public void setEarliestCap(int earliestCap) {
            this.earliestCap = earliestCap;
        }

        public int getMaxSessions() {
            return maxSessions;
        }

        public void setMaxSessions(int maxSessions) {
            this.maxSessions = maxSessions;
        }

        public Duration getIdleTtl() {
            return idleTtl;
        }

        public void setIdleTtl(Duration idleTtl) {
            this.idleTtl = idleTtl;
        }

        public int getMaxPayloadBytes() {
            return maxPayloadBytes;
        }

        public void setMaxPayloadBytes(int maxPayloadBytes) {
            this.maxPayloadBytes = maxPayloadBytes;
        }
    }
}
