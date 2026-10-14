package com.acme.kafkatester.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;

import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.consumer.ConsumerSession.Event;
import com.acme.kafkatester.consumer.ConsumerSession.Info;
import com.acme.kafkatester.consumer.ConsumerSession.OffsetMode;
import com.acme.kafkatester.consumer.ConsumerSession.PollResult;
import com.acme.kafkatester.consumer.ConsumerSession.Status;
import com.acme.kafkatester.consumer.ConsumerSessionManager;
import com.acme.kafkatester.consumer.ConsumerSessionNotFoundException;

/**
 * Consumer session lifecycle against a real broker: LATEST {@code seekToEnd()}, EARLIEST cap
 * auto-stop, stop teardown (unregister AND stop), and cursor polling that never re-transmits.
 */
@EnabledIfDockerAvailable
@SpringBootTest(properties = "kafkatester.consumer.earliest-cap=3")
@AutoConfigureMockMvc
class ConsumerLifecycleIntegrationTest extends AbstractBrokerIntegrationTest {

    @Autowired ConsumerSessionManager sessions;
    @Autowired KafkaListenerEndpointRegistry listenerRegistry;
    @Autowired SchemaCatalog catalog;

    private String payload() {
        return catalog.generateExample("PingEvent", "AVRO");
    }

    /** Creates the topic, registers the Avro schema through the API and returns the topic name. */
    private String preparedTopic(String prefix) throws Exception {
        String topic = uniqueTopic(prefix);
        createTopic(topic);
        registerViaApi("AVRO", topic);
        return topic;
    }

    private Info startAndAwaitListening(String topic, OffsetMode mode) throws Exception {
        Info info = sessions.startSession(topic, MessageFormat.AVRO, mode);
        await("session " + info.id() + " LISTENING", () -> sessions.listSessions().stream()
                .anyMatch(i -> i.id().equals(info.id()) && i.status() != Status.STARTING));
        return info;
    }

    private PollResult awaitEvents(String id, long since, int atLeast) throws Exception {
        List<PollResult> holder = new ArrayList<>();
        await(atLeast + " event(s) after cursor " + since, () -> {
            PollResult r = sessions.poll(id, since);
            holder.clear();
            holder.add(r);
            return r.events().size() >= atLeast;
        });
        return holder.get(0);
    }

    @Test
    void latestSessionSeeksToEndAndNeverReplaysEarlierRecords() throws Exception {
        String topic = preparedTopic("latest");
        send(topic, "AVRO", "old-1", payload());
        send(topic, "AVRO", "old-2", payload());

        Info info = startAndAwaitListening(topic, OffsetMode.LATEST);
        try {
            assertThat(info.groupId()).startsWith("kafka-tester-");
            assertThat(info.effectiveCap()).isNull();
            send(topic, "AVRO", "new-1", payload());
            send(topic, "AVRO", "new-2", payload());

            PollResult result = awaitEvents(info.id(), 0, 2);
            assertThat(result.events()).extracting(Event::key).containsExactly("new-1", "new-2");
            assertThat(result.events()).allSatisfy(e -> {
                assertThat(e.error()).isNull();
                assertThat(e.payload()).isNotBlank();
            });
        } finally {
            sessions.stopSession(info.id());
        }
    }

    @Test
    void cursorPollingNeverRetransmitsASeenRecord() throws Exception {
        String topic = preparedTopic("cursor");
        Info info = startAndAwaitListening(topic, OffsetMode.LATEST);
        try {
            Set<String> seen = new HashSet<>();
            long cursor = 0;
            for (int i = 1; i <= 3; i++) {
                send(topic, "AVRO", "m-" + i, payload());
                PollResult r = awaitEvents(info.id(), cursor, 1);
                assertThat(r.events()).extracting(Event::key).containsExactly("m-" + i);
                r.events().forEach(e -> assertThat(seen.add(e.partition() + ":" + e.offset()))
                        .as("record %s:%s transmitted twice", e.partition(), e.offset()).isTrue());
                cursor = r.nextOffset();
                // polling again with the new cursor yields nothing until a new record arrives
                assertThat(sessions.poll(info.id(), cursor).events()).isEmpty();
            }
            assertThat(seen).hasSize(3);
            assertThat(sessions.poll(info.id(), cursor).missed()).isZero();
        } finally {
            sessions.stopSession(info.id());
        }
    }

    @Test
    void earliestSessionAutoStopsAtTheConfiguredCap() throws Exception {
        String topic = preparedTopic("earliest");
        for (int i = 1; i <= 6; i++) {
            send(topic, "AVRO", "e-" + i, payload());
        }

        Info info = sessions.startSession(topic, MessageFormat.AVRO, OffsetMode.EARLIEST);
        assertThat(info.effectiveCap()).isEqualTo(3);
        MessageListenerContainer container = listenerRegistry.getListenerContainer(info.groupId());
        assertThat(container).isNotNull();

        await("CAP_REACHED", () -> sessions.listSessions().stream()
                .anyMatch(i -> i.id().equals(info.id()) && i.status() == Status.CAP_REACHED));
        PollResult result = sessions.poll(info.id(), 0);
        assertThat(result.events()).extracting(Event::key).containsExactly("e-1", "e-2", "e-3");
        assertThat(result.eventCount()).isEqualTo(3);

        // the container is torn down automatically (unregistered and stopped), history stays readable
        await("auto teardown", () -> listenerRegistry.getListenerContainer(info.groupId()) == null && !container.isRunning());

        sessions.stopSession(info.id()); // removing the cap-reached session must still succeed once
        assertThatThrownBy(() -> sessions.stopSession(info.id())).isInstanceOf(ConsumerSessionNotFoundException.class);
    }

    @Test
    void stopTearsDownBothTheRegistryEntryAndTheContainer() throws Exception {
        String topic = preparedTopic("stop");
        Info info = startAndAwaitListening(topic, OffsetMode.LATEST);
        MessageListenerContainer container = listenerRegistry.getListenerContainer(info.groupId());
        assertThat(container).isNotNull();
        assertThat(container.isRunning()).isTrue();

        sessions.stopSession(info.id());

        assertThat(listenerRegistry.getListenerContainer(info.groupId())).as("unregistered").isNull();
        assertThat(container.isRunning()).as("stopped").isFalse();
        assertThat(sessions.listSessions()).extracting(Info::id).doesNotContain(info.id());
        assertThatThrownBy(() -> sessions.poll(info.id(), 0)).isInstanceOf(ConsumerSessionNotFoundException.class);
        assertThatThrownBy(() -> sessions.stopSession(info.id())).isInstanceOf(ConsumerSessionNotFoundException.class);
    }
}
