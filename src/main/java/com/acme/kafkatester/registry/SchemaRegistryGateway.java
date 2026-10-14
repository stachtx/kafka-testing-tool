package com.acme.kafkatester.registry;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.acme.kafkatester.catalog.EventTypeDescriptor;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

/**
 * Read-only face of the Schema Registry (ADR-003, ADR-007): safe serializer/deserializer defaults,
 * lookups and the registration preview. This class never calls the registry's register API; the
 * only write path in the codebase is {@link SchemaRegistrationService}.
 */
@Component
public class SchemaRegistryGateway {

    public static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
    public static final String USE_LATEST_VERSION = "use.latest.version";
    public static final String LATEST_COMPATIBILITY_STRICT = "latest.compatibility.strict";

    /** Subject naming pinned for values; consistent with {@link SubjectNamingStrategy#topicValue()}. */
    public static final String TOPIC_NAME_STRATEGY = TopicNameStrategy.class.getName();

    /**
     * TTL (seconds) of the "latest schema" caches. Confluent 8.3.2 serializer default is -1 (never expires),
     * which would keep writing with a stale version after the UI registers a new one; the shared client
     * and the serializers use this same short TTL so the producer guard and the serializer agree.
     */
    public static final int LATEST_CACHE_TTL_SEC = 5;

    /**
     * Serializer: never auto-register, write with the registry's latest version, and require that the
     * latest version is compatible with the object being written (ADR-007).
     */
    private static final Map<String, Object> SERIALIZER_DEFAULTS = Collections.unmodifiableMap(new LinkedHashMap<>(Map.of(
            AUTO_REGISTER_SCHEMAS, false,
            USE_LATEST_VERSION, true,
            LATEST_COMPATIBILITY_STRICT, true,
            AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL, LATEST_CACHE_TTL_SEC,
            AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TOPIC_NAME_STRATEGY)));

    /**
     * Deserializer: never auto-register and decode by the writer schema id embedded in each record
     * ({@code use.latest.version=false}), so a record decodes even if its subject differs.
     */
    private static final Map<String, Object> DESERIALIZER_DEFAULTS = Collections.unmodifiableMap(new LinkedHashMap<>(Map.of(
            AUTO_REGISTER_SCHEMAS, false,
            USE_LATEST_VERSION, false,
            AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TOPIC_NAME_STRATEGY)));

    /** Latest registry version of a subject. */
    public record SchemaVersionInfo(String subject, int version, int id, String schemaType, String schema) {
    }

    private final SchemaRegistryClient client;
    private final LocalSchemas localSchemas;
    private final SubjectNamingStrategy subjectNaming;

    SchemaRegistryGateway(SchemaRegistryClient client, SubjectNamingStrategy subjectNaming,
            LocalSchemas localSchemas) {
        this.client = client;
        this.subjectNaming = subjectNaming;
        this.localSchemas = localSchemas;
    }

    // ---- safe defaults (ADR-007) ------------------------------------------------------------

    /** Immutable serializer defaults: auto-register off, latest version, strict, TopicNameStrategy. */
    public static Map<String, Object> serializerSafeDefaults() {
        return SERIALIZER_DEFAULTS;
    }

    /** Immutable deserializer defaults: auto-register off, writer schema (no latest), TopicNameStrategy. */
    public static Map<String, Object> deserializerSafeDefaults() {
        return DESERIALIZER_DEFAULTS;
    }

    /**
     * Serializer config: {@code base} (e.g. {@code KafkaSecurityConfig.producerProperties()}) with the
     * serializer safe defaults forced on top, so a stray {@code auto.register.schemas=true} cannot survive.
     */
    public Map<String, Object> serializerConfig(Map<String, ?> base) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }
        merged.putAll(SERIALIZER_DEFAULTS);
        return merged;
    }

    /**
     * Deserializer config: {@code base} with the deserializer safe defaults forced on top
     * ({@code use.latest.version=false} always, even if an operator supplied {@code true}).
     */
    public Map<String, Object> deserializerConfig(Map<String, ?> base) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (base != null) {
            merged.putAll(base);
        }
        merged.putAll(DESERIALIZER_DEFAULTS);
        return merged;
    }

    /** The shared registry client (read use only; registration goes through the Service). */
    public SchemaRegistryClient client() {
        return client;
    }

    // ---- subject naming ---------------------------------------------------------------------

    public String subjectFor(String topic, String eventType) {
        return subjectNaming.subjectFor(topic, eventType);
    }

    // ---- read path --------------------------------------------------------------------------

    /** Latest version of {@code subject}; empty when the subject does not exist. */
    public Optional<SchemaVersionInfo> latestVersion(String subject) {
        try {
            SchemaMetadata m = client.getLatestSchemaMetadata(subject);
            return Optional.of(new SchemaVersionInfo(subject, m.getVersion(), m.getId(),
                    m.getSchemaType() == null ? "AVRO" : m.getSchemaType(), m.getSchema()));
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                return Optional.empty();
            }
            throw SchemaRegistryException.failed("latest version lookup", e);
        } catch (IOException e) {
            throw SchemaRegistryException.failed("latest version lookup", e);
        }
    }

    /** Schema registered under {@code id}. */
    public ParsedSchema schemaById(int id) {
        try {
            return client.getSchemaById(id);
        } catch (RestClientException | IOException e) {
            throw SchemaRegistryException.failed("schema lookup by id", e);
        }
    }

    /** Effective compatibility mode for {@code subject}: subject level, else the global default. */
    public String compatibilityMode(String subject) {
        try {
            return client.getCompatibility(subject);
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                return globalCompatibility();
            }
            throw SchemaRegistryException.failed("compatibility lookup", e);
        } catch (IOException e) {
            throw SchemaRegistryException.failed("compatibility lookup", e);
        }
    }

    private String globalCompatibility() {
        try {
            return client.getCompatibility(null);
        } catch (RestClientException | IOException e) {
            throw SchemaRegistryException.failed("compatibility lookup", e);
        }
    }

    // ---- producer guard (W1) ----------------------------------------------------------------

    /**
     * Read-only pre-send guard: the local catalog schema for {@code eventType}/{@code format} must equal
     * the registry's latest version of the topic's value subject, because the producer encodes against the
     * local schema while the serializer writes with the registry's latest.
     *
     * @throws SchemaRegistryException (409) subject missing or schemas differ; (502-mapped) registry unreachable
     */
    public void assertLocalMatchesRegistryLatest(String eventType, String format, String topic) {
        EventTypeDescriptor d = localSchemas.describe(eventType, format);
        String subject = subjectNaming.subjectFor(topic, d.name());
        SchemaVersionInfo info = latestVersion(subject).orElseThrow(() -> new SchemaRegistryException(
                "no schema registered for subject " + subject + "; register it first", null, 409));
        ParsedSchema remote;
        try {
            remote = client.getSchemaBySubjectAndId(subject, info.id());
        } catch (RestClientException | IOException e) {
            throw SchemaRegistryException.failed("schema lookup", e);
        }
        ParsedSchema local = localSchemas.parsed(d);
        if (!remote.canonicalString().equals(local.canonicalString())) {
            throw new SchemaRegistryException("Local schema for " + d.name() + " differs from registry latest (v"
                    + info.version() + ") of subject " + subject
                    + "; register the new version via the Event Catalog first", null, 409);
        }
    }

    // ---- preview ----------------------------------------------------------------------------

    /** Preview for the event type's default topic and first matching format. */
    public RegistrationPreview preview(String eventType) {
        return preview(eventType, null, null);
    }

    /**
     * Strictly read-only: compares the local schema against the registry latest and asks the registry
     * for a compatibility verdict. {@code format} and {@code topic} may be null (first matching
     * format / the event type's default topic).
     *
     * @throws com.acme.kafkatester.catalog.UnknownEventTypeException unknown event type
     * @throws SchemaRegistryException registry read failed, or no topic can be determined
     */
    public RegistrationPreview preview(String eventType, String format, String topic) {
        EventTypeDescriptor d = localSchemas.describe(eventType, format);
        String effectiveTopic = topic == null || topic.isBlank() ? d.defaultTopic() : topic;
        if (effectiveTopic == null || effectiveTopic.isBlank()) {
            throw new SchemaRegistryException("No topic given and event type '" + d.name() + "' declares no default topic");
        }
        String subject = subjectNaming.subjectFor(effectiveTopic, d.name());
        ParsedSchema local = localSchemas.parsed(d);
        String localText = LocalSchemas.display(local);
        String mode = compatibilityMode(subject);

        Optional<SchemaVersionInfo> latest = latestVersion(subject);
        if (latest.isEmpty()) {
            return new RegistrationPreview(d.name(), d.format(), effectiveTopic, subject, true, null, null, null,
                    localText, mode, true, List.of(), false);
        }
        SchemaVersionInfo info = latest.get();
        try {
            ParsedSchema remote = client.getSchemaBySubjectAndId(subject, info.id());
            String remoteText = LocalSchemas.display(remote);
            boolean identical = remote.canonicalString().equals(local.canonicalString());
            List<String> problems = client.testCompatibilityVerbose(subject, local);
            return new RegistrationPreview(d.name(), d.format(), effectiveTopic, subject, false, info.version(),
                    info.id(), remoteText, localText, mode, problems == null || problems.isEmpty(),
                    problems, identical);
        } catch (RestClientException | IOException e) {
            throw SchemaRegistryException.failed("preview", e);
        }
    }
}
