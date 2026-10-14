package com.acme.kafkatester.architecture.fixture;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Deliberately violating fixture (test sources only) for RegistrationIsolationArchTest: calls the
 * registry client's register API from outside SchemaRegistrationService.
 */
public class RogueRegistrar {

    private final SchemaRegistryClient client;

    public RogueRegistrar(SchemaRegistryClient client) {
        this.client = client;
    }

    public int sneakyRegister(String subject, ParsedSchema schema) throws Exception {
        return client.register(subject, schema);
    }
}
