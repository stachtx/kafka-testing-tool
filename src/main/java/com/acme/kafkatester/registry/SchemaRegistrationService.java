package com.acme.kafkatester.registry;

import java.io.IOException;

import org.springframework.stereotype.Service;

import com.acme.kafkatester.catalog.EventTypeDescriptor;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * THE one write path to the Schema Registry (ADR-003). Contains no preview or diff logic. The
 * constructor is package-private on purpose; the only class meant to inject this bean is
 * {@code web.rest.SchemaRegistrationController} (enforced by the Group 12 ArchUnit test, which also
 * asserts that no other class references the client's {@code register} API).
 */
@Service
public class SchemaRegistrationService {

    /** Outcome of a successful registration. */
    public record RegistrationResult(String subject, int schemaId, int version, String eventType, String format) {
    }

    private final SchemaRegistryClient client;
    private final LocalSchemas localSchemas;
    private final SubjectNamingStrategy subjectNaming;

    SchemaRegistrationService(SchemaRegistryClient client, LocalSchemas localSchemas,
            SubjectNamingStrategy subjectNaming) {
        this.client = client;
        this.localSchemas = localSchemas;
        this.subjectNaming = subjectNaming;
    }

    /**
     * Registers the catalog schema of {@code eventType} under the subject derived from {@code topic}.
     *
     * @param format optional ({@code AVRO}/{@code PROTOBUF}); null = first matching format
     * @param topic  optional; null = the event type's default topic
     * @throws SchemaRegistryException registry rejected or failed the write (incl. incompatible schema)
     */
    public RegistrationResult register(String eventType, String format, String topic) {
        EventTypeDescriptor d = localSchemas.describe(eventType, format);
        String effectiveTopic = topic == null || topic.isBlank() ? d.defaultTopic() : topic;
        if (effectiveTopic == null || effectiveTopic.isBlank()) {
            throw new SchemaRegistryException("No topic given and event type '" + d.name() + "' declares no default topic");
        }
        String subject = subjectNaming.subjectFor(effectiveTopic, d.name());
        ParsedSchema schema = localSchemas.parsed(d);
        try {
            int id = client.register(subject, schema);
            int version = client.getVersion(subject, schema);
            return new RegistrationResult(subject, id, version, d.name(), d.format());
        } catch (RestClientException | IOException e) {
            throw SchemaRegistryException.failed("registration", e);
        }
    }
}
