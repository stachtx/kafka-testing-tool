package com.acme.kafkatester.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.listener.MessageListenerContainer;

import com.acme.kafkatester.KafkaTesterProperties;
import com.acme.kafkatester.codec.AvroSchemaCodec;
import com.acme.kafkatester.codec.CodecRegistry;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.codec.ProtobufSchemaCodec;
import com.acme.kafkatester.consumer.ConsumerSession.Info;
import com.acme.kafkatester.consumer.ConsumerSession.OffsetMode;
import com.acme.kafkatester.consumer.ConsumerSession.PollResult;
import com.acme.kafkatester.consumer.ConsumerSession.Status;
import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;

/** Docker-free tests: the listener container registry and Confluent deserializer are mocked. */
class ConsumerSessionManagerTest {

    private static final Schema PING = SchemaBuilder.record("Ping").fields().requiredString("msg").endRecord();

    private KafkaListenerEndpointRegistry registry;
    private MessageListenerContainer container;
    private Deserializer<Object> valueDeserializer;
    private KafkaTesterProperties properties;
    private ConsumerSessionManager manager;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        registry = mock(KafkaListenerEndpointRegistry.class);
        container = mock(MessageListenerContainer.class);
        when(registry.getListenerContainer(any())).thenReturn(container);
        when(registry.unregisterListenerContainer(any())).thenReturn(container);

        valueDeserializer = mock(Deserializer.class);
        when(valueDeserializer.deserialize(any(), any(), any(byte[].class))).thenAnswer(inv -> {
            byte[] bytes = inv.getArgument(2);
            String text = new String(bytes, StandardCharsets.UTF_8);
            if (text.equals("bad")) {
                throw new IllegalStateException("Error retrieving Avro schema for id 99");
            }
            GenericData.Record r = new GenericData.Record(PING);
            r.put("msg", text);
            return r;
        });
        ValueDeserializerFactory deserializers = mock(ValueDeserializerFactory.class);
        when(deserializers.create(any(), anyMap())).thenReturn(valueDeserializer);

        KafkaSecurityConfig security = mock(KafkaSecurityConfig.class);
        when(security.consumerProperties()).thenReturn(new java.util.LinkedHashMap<>(Map.of(
                "bootstrap.servers", "localhost:9092", "schema.registry.url", "http://localhost:8081")));
        SchemaRegistryGateway gateway = mock(SchemaRegistryGateway.class);
        when(gateway.deserializerConfig(any())).thenAnswer(inv -> inv.getArgument(0));

        properties = new KafkaTesterProperties();
        properties.getConsumer().setEarliestCap(3);

        ValueDecoder decoder = new ValueDecoder(new CodecRegistry(List.of(new AvroSchemaCodec(), new ProtobufSchemaCodec())));
        // direct executor: cap-triggered teardown runs inline, keeping the test deterministic
        manager = new ConsumerSessionManager(registry, new SessionContainerFactory(), deserializers, decoder,
                security, gateway, properties, Runnable::run, clock);
    }

    /** Controllable clock for the idle sweeper. */
    private static final class MutableClock extends java.time.Clock {
        private java.time.Instant now = java.time.Instant.parse("2026-01-01T00:00:00Z");

        void advance(java.time.Duration d) {
            now = now.plus(d);
        }

        @Override public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override public java.time.Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override public java.time.Instant instant() {
            return now;
        }
    }

    private final MutableClock clock = new MutableClock();

    @Test
    void startBeyondMaxSessionsIsRejectedAndFreedSlotCanBeReused() {
        properties.getConsumer().setMaxSessions(2);
        Info a = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);

        assertThatThrownBy(() -> manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST))
                .isInstanceOf(ConsumerLimitReachedException.class)
                .hasMessageContaining("2");
        assertThat(ConsumerLimitReachedException.ERROR_CODE).isEqualTo("CONSUMER_LIMIT_REACHED");
        assertThat(manager.listSessions()).hasSize(2);

        manager.stopSession(a.id());
        manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        assertThat(manager.listSessions()).hasSize(2);
    }

    @Test
    void idleSweeperTearsDownSessionsNotPolledWithinTtlButKeepsPolledOnes() {
        properties.getConsumer().setIdleTtl(java.time.Duration.ofMinutes(30));
        Info idle = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        Info active = manager.startSession("topic-b", MessageFormat.AVRO, OffsetMode.LATEST);

        clock.advance(java.time.Duration.ofMinutes(20));
        manager.poll(active.id(), 0);
        manager.sweepIdle(); // nothing idle for 30m yet
        assertThat(manager.listSessions()).hasSize(2);

        clock.advance(java.time.Duration.ofMinutes(15)); // idle: 35m, active: 15m
        manager.sweepIdle();

        assertThat(manager.listSessions()).extracting(Info::id).containsExactly(active.id());
        verify(registry).unregisterListenerContainer(idle.groupId());
        verify(container).stop();
        assertThatThrownBy(() -> manager.poll(idle.id(), 0)).isInstanceOf(ConsumerSessionNotFoundException.class);
    }

    @Test
    void oversizedDecodedPayloadIsTruncatedWithMarkerAndFlag() {
        properties.getConsumer().setMaxPayloadBytes(64);
        Info info = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        SessionRecordListener listener = listenerOfLastStart();
        listener.onPartitionsAssigned(Map.of(new TopicPartition("topic-a", 0), 0L), mock(ConsumerSeekCallback.class));

        deliver(listener, 0, "x".repeat(500));
        deliver(listener, 1, "small");

        List<ConsumerSession.Event> events = manager.poll(info.id(), 0).events();
        assertThat(events.get(0).payloadTruncated()).isTrue();
        assertThat(events.get(0).payload().getBytes(StandardCharsets.UTF_8).length)
                .isLessThanOrEqualTo(64 + SessionRecordListener.TRUNCATION_MARKER.length());
        assertThat(events.get(0).payload()).endsWith(SessionRecordListener.TRUNCATION_MARKER);
        assertThat(events.get(1).payloadTruncated()).isFalse();
        assertThat(events.get(1).payload()).contains("small");
    }

    @Test
    void truncationDoesNotSplitMultiByteCharacters() {
        String cut = SessionRecordListener.truncate("aé".repeat(20), 5); // a(1) é(2) a(1) é(2): 5th byte splits é
        assertThat(cut).startsWith("aéa").doesNotContain("�");
    }

    @Test
    void decodeErrorTextIsSanitizedAndCapped() {
        org.mockito.Mockito.doThrow(new IllegalStateException(
                "registry http://user:pw@host:8081 failed password=abc " + "z".repeat(1000)))
                .when(valueDeserializer).deserialize(any(), any(), any(byte[].class));
        Info info = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        SessionRecordListener listener = listenerOfLastStart();
        listener.onPartitionsAssigned(Map.of(new TopicPartition("topic-a", 0), 0L), mock(ConsumerSeekCallback.class));

        deliver(listener, 0, "whatever");

        String error = manager.poll(info.id(), 0).events().getFirst().error();
        assertThat(error).contains("IllegalStateException").contains("Could not decode AVRO")
                .doesNotContain("user:pw").doesNotContain("password=abc").doesNotContain("=abc");
        assertThat(error).contains("***@");
        assertThat(error.length()).isLessThan(400);
    }

    @Test
    void latestSeeksToEndOnlyForPartitionsAssignedForTheFirstTime() {
        manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        SessionRecordListener listener = listenerOfLastStart();
        TopicPartition p0 = new TopicPartition("topic-a", 0);
        TopicPartition p1 = new TopicPartition("topic-a", 1);
        ConsumerSeekCallback first = mock(ConsumerSeekCallback.class);
        listener.onPartitionsAssigned(Map.of(p0, 0L), first);
        verify(first).seekToEnd(Set.of(p0));

        // rebalance: p0 is kept (must not be re-sought), p1 is new
        ConsumerSeekCallback second = mock(ConsumerSeekCallback.class);
        listener.onPartitionsAssigned(Map.of(p0, 5L, p1, 0L), second);
        verify(second).seekToEnd(Set.of(p1));

        // nothing new: no seek at all
        ConsumerSeekCallback third = mock(ConsumerSeekCallback.class);
        listener.onPartitionsAssigned(Map.of(p0, 7L, p1, 3L), third);
        verify(third, never()).seekToEnd(any(java.util.Collection.class));
    }

    private SessionRecordListener listenerOfLastStart() {
        ArgumentCaptor<SessionContainerFactory.Endpoint> captor =
                ArgumentCaptor.forClass(SessionContainerFactory.Endpoint.class);
        verify(registry, org.mockito.Mockito.atLeastOnce()).registerListenerContainer(captor.capture(), any(), eq(true));
        return captor.getValue().listener();
    }

    private static void deliver(SessionRecordListener listener, long offset, String value) {
        listener.onMessage(new ConsumerRecord<>("topic-a", 0, offset, "k" + offset,
                value.getBytes(StandardCharsets.UTF_8)), (org.springframework.kafka.support.Acknowledgment) null);
    }

    @Test
    void historyKeepsNewest100AndCursorPollDoesNotRetransmit() {
        Info info = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        SessionRecordListener listener = listenerOfLastStart();
        listener.onPartitionsAssigned(Map.of(new TopicPartition("topic-a", 0), 0L), mock(ConsumerSeekCallback.class));

        IntStream.range(0, 150).forEach(i -> deliver(listener, i, "m" + i));

        PollResult first = manager.poll(info.id(), 0);
        assertThat(first.events()).hasSize(100);
        assertThat(first.events().getFirst().offset()).isEqualTo(50);
        assertThat(first.events().getLast().offset()).isEqualTo(149);
        assertThat(first.missed()).isEqualTo(50);
        assertThat(first.nextOffset()).isEqualTo(150);
        assertThat(first.eventCount()).isEqualTo(150);

        assertThat(manager.poll(info.id(), first.nextOffset()).events()).isEmpty();

        deliver(listener, 150, "m150");
        deliver(listener, 151, "m151");
        PollResult second = manager.poll(info.id(), first.nextOffset());
        assertThat(second.events()).extracting(ConsumerSession.Event::offset).containsExactly(150L, 151L);
        assertThat(second.missed()).isZero();
        assertThat(second.events().getFirst().payload()).contains("m150");
    }

    @Test
    void groupIdIsKafkaTesterUuidAndUniquePerSession() {
        Info a = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        Info b = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);

        assertThat(a.groupId()).matches("kafka-tester-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        assertThat(a.groupId()).isEqualTo("kafka-tester-" + a.id());
        assertThat(a.groupId()).isNotEqualTo(b.groupId());
        assertThat(manager.listSessions()).hasSize(2);

        ArgumentCaptor<SessionContainerFactory.Endpoint> captor =
                ArgumentCaptor.forClass(SessionContainerFactory.Endpoint.class);
        verify(registry, org.mockito.Mockito.times(2)).registerListenerContainer(captor.capture(), any(), eq(true));
        Map<String, Object> props = captor.getAllValues().getFirst().consumerProps();
        assertThat(props).containsEntry("group.id", a.groupId())
                .containsEntry("auto.offset.reset", "latest")
                .containsEntry("enable.auto.commit", false);
    }

    @Test
    void latestSeeksToEndBeforeStatusBecomesListening() {
        Info info = manager.startSession("topic-a", MessageFormat.PROTOBUF, OffsetMode.LATEST);
        assertThat(info.status()).isEqualTo(Status.STARTING);
        assertThat(info.effectiveCap()).isNull();

        SessionRecordListener listener = listenerOfLastStart();
        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);
        TopicPartition tp = new TopicPartition("topic-a", 0);
        org.mockito.Mockito.doAnswer(inv -> {
            // status must still be STARTING at the moment the seek is requested
            assertThat(manager.listSessions().getFirst().status()).isEqualTo(Status.STARTING);
            return null;
        }).when(callback).seekToEnd(Set.of(tp));

        listener.onPartitionsAssigned(Map.of(tp, 0L), callback);

        verify(callback).seekToEnd(Set.of(tp));
        assertThat(manager.listSessions().getFirst().status()).isEqualTo(Status.LISTENING);
    }

    @Test
    void earliestDoesNotSeekAndAutoStopsAtConfiguredCap() {
        Info info = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.EARLIEST);
        assertThat(info.effectiveCap()).isEqualTo(3);

        SessionRecordListener listener = listenerOfLastStart();
        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);
        listener.onPartitionsAssigned(Map.of(new TopicPartition("topic-a", 0), 0L), callback);
        verify(callback, never()).seekToEnd(any(java.util.Collection.class));

        IntStream.range(0, 5).forEach(i -> deliver(listener, i, "m" + i));

        Info after = manager.listSessions().getFirst();
        assertThat(after.status()).isEqualTo(Status.CAP_REACHED);
        assertThat(after.eventCount()).isEqualTo(3);
        assertThat(manager.poll(info.id(), 0).events()).hasSize(3);
        verify(registry).unregisterListenerContainer(info.groupId());
        verify(container).stop();

        // explicit stop afterwards removes the card and does not tear down twice
        manager.stopSession(info.id());
        verify(container).stop();
        assertThat(manager.listSessions()).isEmpty();
    }

    @Test
    void stopInvokesBothUnregisterAndStopThenIdIsGone() {
        Info info = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);

        manager.stopSession(info.id());

        InOrder order = inOrder(registry, container);
        order.verify(registry).unregisterListenerContainer(info.groupId());
        order.verify(container).stop();
        verify(valueDeserializer).close();
        assertThat(manager.listSessions()).isEmpty();
        assertThatThrownBy(() -> manager.poll(info.id(), 0)).isInstanceOf(ConsumerSessionNotFoundException.class);
        assertThatThrownBy(() -> manager.stopSession(info.id())).isInstanceOf(ConsumerSessionNotFoundException.class);
    }

    @Test
    void stopStillStopsContainerWhenUnregisterFails() {
        Info info = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        when(registry.unregisterListenerContainer(any())).thenThrow(new IllegalStateException("boom"));

        manager.stopSession(info.id());

        verify(container).stop();
    }

    @Test
    void containerRegistrationFailureRaisesStartExceptionAndLeavesNoSessionBehind() {
        org.mockito.Mockito.doThrow(new IllegalStateException("cannot connect password=hunter2"))
                .when(registry).registerListenerContainer(any(), any(), eq(true));

        assertThatThrownBy(() -> manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST))
                .isInstanceOf(ConsumerStartException.class)
                .hasMessageNotContaining("hunter2");

        assertThat(manager.listSessions()).isEmpty();
        verify(registry).unregisterListenerContainer(any());
        verify(valueDeserializer).close();
    }

    @Test
    void unknownIdRaisesSessionNotFound() {
        assertThatThrownBy(() -> manager.poll("nope", 0))
                .isInstanceOf(ConsumerSessionNotFoundException.class)
                .hasMessageContaining("nope");
        assertThatThrownBy(() -> manager.stopSession("nope")).isInstanceOf(ConsumerSessionNotFoundException.class);
        assertThat(ConsumerSessionNotFoundException.ERROR_CODE).isEqualTo("CONSUMER_SESSION_NOT_FOUND");
    }

    @Test
    void undecodableRecordIsStoredRawWithErrorAndSessionKeepsGoing() {
        Info info = manager.startSession("topic-a", MessageFormat.AVRO, OffsetMode.LATEST);
        SessionRecordListener listener = listenerOfLastStart();
        listener.onPartitionsAssigned(Map.of(new TopicPartition("topic-a", 0), 0L), mock(ConsumerSeekCallback.class));

        deliver(listener, 0, "hello");
        deliver(listener, 1, "bad");
        deliver(listener, 2, "world");

        PollResult result = manager.poll(info.id(), 0);
        assertThat(result.status()).isEqualTo(Status.LISTENING);
        assertThat(result.events()).hasSize(3);

        ConsumerSession.Event ok = result.events().get(0);
        assertThat(ok.error()).isNull();
        assertThat(ok.payload()).contains("hello");
        assertThat(ok.key()).isEqualTo("k0");

        ConsumerSession.Event bad = result.events().get(1);
        assertThat(bad.payload()).isNull();
        assertThat(bad.error()).contains("Could not decode AVRO");
        assertThat(bad.rawBase64()).isEqualTo(java.util.Base64.getEncoder().encodeToString("bad".getBytes(StandardCharsets.UTF_8)));

        assertThat(result.events().get(2).payload()).contains("world");
    }
}
