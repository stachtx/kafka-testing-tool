package com.acme.kafkatester.consumer;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.acme.kafkatester.KafkaTesterProperties;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.consumer.ConsumerSession.Info;
import com.acme.kafkatester.consumer.ConsumerSession.OffsetMode;
import com.acme.kafkatester.consumer.ConsumerSession.PollResult;
import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;

import jakarta.annotation.PreDestroy;

/**
 * Owns the dynamic consumer sessions (ADR: random per-session group id, {@code seekToEnd()} on
 * assignment for LATEST, EARLIEST record cap, bounded history, guaranteed teardown).
 *
 * <p>Containers are created by {@link SessionContainerFactory} and registered through
 * {@link KafkaListenerEndpointRegistry#registerListenerContainer}; teardown always calls
 * {@link KafkaListenerEndpointRegistry#unregisterListenerContainer(String)} <em>and</em>
 * {@code container.stop()}. This class must not depend on {@code catalog/}: records are decoded with
 * the registry-resolved schema only.
 */
@Service
public class ConsumerSessionManager {

    public static final String GROUP_PREFIX = "kafka-tester-";

    private static final Logger log = LoggerFactory.getLogger(ConsumerSessionManager.class);

    private final Map<String, ConsumerSession> sessions = new ConcurrentHashMap<>();

    private final KafkaListenerEndpointRegistry registry;
    private final SessionContainerFactory containerFactory;
    private final ValueDeserializerFactory deserializers;
    private final ValueDecoder decoder;
    private final KafkaSecurityConfig securityConfig;
    private final SchemaRegistryGateway gateway;
    private final KafkaTesterProperties properties;
    private final Executor teardownExecutor;
    private final Clock clock;
    private final Object admission = new Object();

    @Autowired
    public ConsumerSessionManager(KafkaListenerEndpointRegistry registry, SessionContainerFactory containerFactory,
            ValueDeserializerFactory deserializers, ValueDecoder decoder, KafkaSecurityConfig securityConfig,
            SchemaRegistryGateway gateway, KafkaTesterProperties properties) {
        this(registry, containerFactory, deserializers, decoder, securityConfig, gateway, properties,
                task -> Thread.ofVirtual().name("consumer-session-teardown").start(task));
    }

    /** Executor seam: cap-triggered teardown must not run on the container's own consumer thread. */
    ConsumerSessionManager(KafkaListenerEndpointRegistry registry, SessionContainerFactory containerFactory,
            ValueDeserializerFactory deserializers, ValueDecoder decoder, KafkaSecurityConfig securityConfig,
            SchemaRegistryGateway gateway, KafkaTesterProperties properties, Executor teardownExecutor) {
        this(registry, containerFactory, deserializers, decoder, securityConfig, gateway, properties,
                teardownExecutor, Clock.systemUTC());
    }

    /** Clock seam: the idle sweeper compares session activity against {@code clock.instant()}. */
    ConsumerSessionManager(KafkaListenerEndpointRegistry registry, SessionContainerFactory containerFactory,
            ValueDeserializerFactory deserializers, ValueDecoder decoder, KafkaSecurityConfig securityConfig,
            SchemaRegistryGateway gateway, KafkaTesterProperties properties, Executor teardownExecutor,
            Clock clock) {
        this.clock = clock;
        this.registry = registry;
        this.containerFactory = containerFactory;
        this.deserializers = deserializers;
        this.decoder = decoder;
        this.securityConfig = securityConfig;
        this.gateway = gateway;
        this.properties = properties;
        this.teardownExecutor = teardownExecutor;
    }

    /**
     * Start a session on {@code topic}. LATEST seeks to the end on assignment; EARLIEST reads from the
     * beginning and auto-stops at {@code kafkatester.consumer.earliest-cap} records.
     *
     * @throws ConsumerStartException when the container or deserializer cannot be created/started
     * @throws ConsumerLimitReachedException when {@code kafkatester.consumer.max-sessions} sessions exist
     */
    public Info startSession(String topic, MessageFormat format, OffsetMode offsetMode) {
        if (topic == null || topic.isBlank() || format == null || offsetMode == null) {
            throw new ConsumerStartException("topic, format and offsetMode are required");
        }
        String id = UUID.randomUUID().toString();
        String groupId = GROUP_PREFIX + id;
        Integer cap = offsetMode == OffsetMode.EARLIEST ? properties.getConsumer().getEarliestCap() : null;
        ConsumerSession session = new ConsumerSession(id, groupId, topic.trim(), format, offsetMode, clock.instant(), cap);

        int maxSessions = properties.getConsumer().getMaxSessions();
        synchronized (admission) { // atomic check-and-reserve so concurrent starts cannot overshoot the limit
            if (sessions.size() >= maxSessions) {
                throw new ConsumerLimitReachedException(maxSessions);
            }
            sessions.put(id, session);
        }

        try {
            Map<String, Object> clientProps = securityConfig.consumerProperties();
            Map<String, Object> deserializerConfig = gateway.deserializerConfig(clientProps);

            Map<String, Object> consumerProps = new LinkedHashMap<>(clientProps);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetMode.name().toLowerCase());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

            Deserializer<Object> valueDeserializer = deserializers.create(format, deserializerConfig);
            SessionRecordListener listener = new SessionRecordListener(session, valueDeserializer, decoder,
                    () -> teardownExecutor.execute(() -> teardown(session)),
                    properties.getConsumer().getMaxPayloadBytes());
            session.cleanup(listener::close);

            SessionContainerFactory.Endpoint endpoint =
                    new SessionContainerFactory.Endpoint(groupId, session.topic(), consumerProps, listener);
            registry.registerListenerContainer(endpoint, containerFactory, true);
            session.container(registry.getListenerContainer(groupId));
        } catch (RuntimeException e) {
            sessions.remove(id);
            teardown(session);
            throw new ConsumerStartException("Could not start consumer session on topic '" + topic + "'", e);
        }
        return session.info();
    }

    /**
     * Stop and remove a session.
     *
     * @throws ConsumerSessionNotFoundException unknown or already stopped id
     */
    public void stopSession(String id) {
        ConsumerSession session = sessions.remove(id);
        if (session == null) {
            throw new ConsumerSessionNotFoundException(id);
        }
        teardown(session);
    }

    /** Metadata of all sessions (including cap-reached ones awaiting removal), oldest first. */
    public List<Info> listSessions() {
        return sessions.values().stream().map(ConsumerSession::info)
                .sorted(Comparator.comparing(Info::startedAt)).toList();
    }

    /**
     * Events newer than {@code sinceOffset} (the previous result's {@code nextOffset}; 0 = from the
     * beginning of the retained history).
     *
     * @throws ConsumerSessionNotFoundException unknown or stopped id
     */
    public PollResult poll(String id, long sinceOffset) {
        ConsumerSession session = require(id);
        session.touch(clock.instant());
        return session.poll(sinceOffset);
    }

    /**
     * Tears down sessions that were not polled for {@code kafkatester.consumer.idle-ttl}. Uses the regular
     * teardown (unregister + stop). Invoked periodically by the scheduler; public listing does not count as
     * activity, only polling does.
     */
    @Scheduled(fixedDelayString = "${kafkatester.consumer.sweep-interval:PT1M}",
            initialDelayString = "${kafkatester.consumer.sweep-interval:PT1M}")
    void sweepIdle() {
        Instant cutoff = clock.instant().minus(properties.getConsumer().getIdleTtl());
        for (ConsumerSession session : List.copyOf(sessions.values())) {
            if (session.lastActivity().isBefore(cutoff) && sessions.remove(session.id(), session)) {
                log.info("Tearing down idle consumer session {} (topic {})", session.id(), session.topic());
                try {
                    teardown(session);
                } catch (RuntimeException e) {
                    log.warn("Idle teardown of {} failed: {}", session.groupId(), e.toString());
                }
            }
        }
    }

    private ConsumerSession require(String id) {
        ConsumerSession session = id == null ? null : sessions.get(id);
        if (session == null) {
            throw new ConsumerSessionNotFoundException(id);
        }
        return session;
    }

    /**
     * Idempotent teardown. Always invokes both {@code registry.unregisterListenerContainer(id)} and
     * {@code container.stop()}; a failure in one never skips the other.
     */
    private void teardown(ConsumerSession session) {
        if (!session.beginTeardown()) {
            return;
        }
        MessageListenerContainer unregistered = null;
        try {
            unregistered = registry.unregisterListenerContainer(session.groupId());
        } catch (RuntimeException e) {
            log.warn("Unregistering listener container {} failed: {}", session.groupId(), e.toString());
        }
        MessageListenerContainer container = unregistered != null ? unregistered : session.container();
        try {
            if (container != null) {
                container.stop();
            }
        } catch (RuntimeException e) {
            log.warn("Stopping listener container {} failed: {}", session.groupId(), e.toString());
        } finally {
            session.runCleanup();
            // a cap-reached session keeps its CAP_REACHED status (history stays readable until removed)
            if (session.status() != ConsumerSession.Status.CAP_REACHED || !sessions.containsKey(session.id())) {
                session.markStopped();
            }
        }
    }

    @PreDestroy
    void shutdown() {
        sessions.values().forEach(this::teardown);
        sessions.clear();
    }
}
