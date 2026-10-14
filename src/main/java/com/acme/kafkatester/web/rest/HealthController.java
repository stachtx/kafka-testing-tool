package com.acme.kafkatester.web.rest;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.acme.kafkatester.KafkaTesterProperties;
import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;
import com.acme.kafkatester.web.dto.HealthDto;
import com.acme.kafkatester.web.dto.HealthDto.Component;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/** Cluster and Schema Registry reachability. Never throws; short timeouts; reveals no hosts or credentials. */
@RestController
@RequestMapping("/api/kafka")
@Tag(name = "Health")
public class HealthController {

    static final int TIMEOUT_MS = 3000;
    private static final Logger log = LoggerFactory.getLogger(HealthController.class);

    private final KafkaSecurityConfig securityConfig;
    private final SchemaRegistryGateway gateway;
    private final KafkaTesterProperties properties;

    public HealthController(KafkaSecurityConfig securityConfig, SchemaRegistryGateway gateway,
            KafkaTesterProperties properties) {
        this.securityConfig = securityConfig;
        this.gateway = gateway;
        this.properties = properties;
    }

    @GetMapping("/health")
    @Operation(summary = "Kafka cluster and Schema Registry reachability")
    public HealthDto health() {
        CompletableFuture<Component> kafka = CompletableFuture.supplyAsync(this::checkKafka)
                .completeOnTimeout(new Component(false, "timed out"), TIMEOUT_MS + 1000L, TimeUnit.MILLISECONDS);
        CompletableFuture<Component> registry = CompletableFuture.supplyAsync(this::checkRegistry)
                .completeOnTimeout(new Component(false, "timed out"), TIMEOUT_MS + 1000L, TimeUnit.MILLISECONDS);
        Component k = join(kafka);
        Component r = join(registry);
        String status = k.reachable() && r.reachable() ? "UP" : k.reachable() || r.reachable() ? "DEGRADED" : "DOWN";
        return new HealthDto(status, k, r, properties.getConsumer().getEarliestCap());
    }

    private static Component join(CompletableFuture<Component> f) {
        try {
            return f.get(TIMEOUT_MS + 2000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Component(false, "interrupted");
        } catch (Exception e) {
            return new Component(false, "check failed");
        }
    }

    private Component checkKafka() {
        try {
            Map<String, Object> config = new LinkedHashMap<>();
            Map<String, Object> source = securityConfig.producerProperties();
            for (String name : AdminClientConfig.configNames()) {
                if (source.containsKey(name)) {
                    config.put(name, source.get(name));
                }
            }
            config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
            config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
            config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
            config.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, TIMEOUT_MS);
            try (Admin admin = Admin.create(config)) {
                int nodes = admin.describeCluster().nodes().get(TIMEOUT_MS, TimeUnit.MILLISECONDS).size();
                return new Component(true, nodes + " broker(s) reachable");
            }
        } catch (Exception e) {
            log.warn("Kafka health check failed: {}", e.getClass().getSimpleName());
            return new Component(false, "unreachable (" + e.getClass().getSimpleName() + ")");
        }
    }

    private Component checkRegistry() {
        try {
            gateway.client().getAllSubjects();
            return new Component(true, "reachable");
        } catch (Exception e) {
            log.warn("Schema Registry health check failed: {}", e.getClass().getSimpleName());
            return new Component(false, "unreachable (" + e.getClass().getSimpleName() + ")");
        }
    }
}
