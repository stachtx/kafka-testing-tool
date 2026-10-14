package com.acme.kafkatester.web.dto;

/**
 * {@code status}: UP (both reachable), DEGRADED (one reachable), DOWN (none). Details never contain
 * hosts, credentials or stack traces. {@code earliestCap} is the configured EARLIEST consumer record cap
 * (lets the UI name the exact cap in its confirmation dialog).
 */
public record HealthDto(String status, Component kafka, Component schemaRegistry, int earliestCap) {

    public record Component(boolean reachable, String detail) {
    }
}
