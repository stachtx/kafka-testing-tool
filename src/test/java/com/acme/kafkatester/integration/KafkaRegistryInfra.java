package com.acme.kafkatester.integration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Throw-away Kafka broker and Schema Registry for the integration tests (Testcontainers only; no real
 * DEV cluster is ever contacted). Started lazily and once per JVM, shared by all integration test
 * classes; the Testcontainers reaper (Ryuk) removes the containers when the JVM exits.
 *
 * <p>Image tags are pinned to the Confluent Platform 8.3.x family matching the Confluent client
 * libraries (8.3.2) used by the application; both tags exist on Docker Hub.
 */
final class KafkaRegistryInfra {

    static final String KAFKA_IMAGE = "confluentinc/cp-kafka:8.3.2";
    static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:8.3.2";

    private static Network network;
    private static ConfluentKafkaContainer kafka;
    private static GenericContainer<?> schemaRegistry;

    private KafkaRegistryInfra() {
    }

    static synchronized void ensureStarted() {
        if (schemaRegistry != null) {
            return;
        }
        network = Network.newNetwork();
        kafka = new ConfluentKafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("kafka");
        kafka.start();

        GenericContainer<?> registry = new GenericContainer<>(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                // ConfluentKafkaContainer exposes its in-network BROKER listener on 9093
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9093")
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        registry.start();
        schemaRegistry = registry;
    }

    static String bootstrapServers() {
        ensureStarted();
        return kafka.getBootstrapServers();
    }

    static String registryUrl() {
        ensureStarted();
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    }
}
