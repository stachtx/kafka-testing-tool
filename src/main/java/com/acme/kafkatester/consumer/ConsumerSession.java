package com.acme.kafkatester.consumer;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.kafka.listener.MessageListenerContainer;

import com.acme.kafkatester.codec.MessageFormat;

/**
 * One live consumer session: listener container reference, metadata and a bounded in-memory history
 * of the newest {@value #HISTORY_LIMIT} records.
 *
 * <p>Cursor semantics: every stored event gets a per-session, monotonically increasing sequence
 * number starting at 1 ({@link Event#sequence()}). {@link #poll(long)} returns the retained events
 * whose sequence is greater than {@code sinceOffset} (0 or negative = everything retained), plus the
 * {@code nextOffset} the caller sends on its next poll. Kafka partition offsets are not a usable
 * cursor across partitions, so the real {@code offset}/{@code partition} travel on each event.
 */
public final class ConsumerSession {

    public static final int HISTORY_LIMIT = 100;

    public enum OffsetMode { LATEST, EARLIEST }

    public enum Status {
        /** Container created, partitions not assigned (and for LATEST, not yet at the end). */
        STARTING,
        LISTENING,
        /** EARLIEST session hit its cap; the container is torn down but history stays readable. */
        CAP_REACHED,
        STOPPED
    }

    /** A consumed record, decoded to JSON or kept raw with an error. */
    public record Event(long sequence, String topic, int partition, long offset, long timestamp, String key,
            Map<String, String> headers, String payload, String error, String rawBase64,
            boolean payloadTruncated) {
    }

    /** Session metadata snapshot. {@code effectiveCap} is null for LATEST sessions. */
    public record Info(String id, String groupId, String topic, MessageFormat format, OffsetMode offsetMode,
            Instant startedAt, long eventCount, Status status, Integer effectiveCap) {
    }

    /** Result of a cursor poll. {@code missed} = events evicted from history that the caller never saw. */
    public record PollResult(String sessionId, List<Event> events, long nextOffset, long missed, Status status,
            long eventCount) {
    }

    private final String id;
    private final String groupId;
    private final String topic;
    private final MessageFormat format;
    private final OffsetMode offsetMode;
    private final Instant startedAt;
    private final Integer effectiveCap;

    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<Event> history = new ArrayDeque<>(HISTORY_LIMIT + 1);
    private long lastSequence;

    private volatile Instant lastActivity;
    private volatile Status status = Status.STARTING;
    private volatile MessageListenerContainer container;
    private volatile Runnable cleanup = () -> { };
    private final AtomicBoolean tornDown = new AtomicBoolean();

    ConsumerSession(String id, String groupId, String topic, MessageFormat format, OffsetMode offsetMode,
            Instant startedAt, Integer effectiveCap) {
        this.id = id;
        this.groupId = groupId;
        this.topic = topic;
        this.format = format;
        this.offsetMode = offsetMode;
        this.startedAt = startedAt;
        this.effectiveCap = effectiveCap;
        this.lastActivity = startedAt;
    }

    /** Last time a client polled this session (or the start time); drives the idle-TTL sweeper. */
    Instant lastActivity() {
        return lastActivity;
    }

    void touch(Instant now) {
        this.lastActivity = now;
    }

    public String id() {
        return id;
    }

    public String groupId() {
        return groupId;
    }

    public String topic() {
        return topic;
    }

    public MessageFormat format() {
        return format;
    }

    public OffsetMode offsetMode() {
        return offsetMode;
    }

    public Integer effectiveCap() {
        return effectiveCap;
    }

    public Status status() {
        return status;
    }

    /** Total records received (not bounded by the history size). */
    public long eventCount() {
        lock.lock();
        try {
            return lastSequence;
        } finally {
            lock.unlock();
        }
    }

    public Info info() {
        return new Info(id, groupId, topic, format, offsetMode, startedAt, eventCount(), status, effectiveCap);
    }

    /** Append an event (assigning its sequence) and trim history to the newest 100. Returns the new count. */
    long append(Event template) {
        lock.lock();
        try {
            lastSequence++;
            history.addLast(new Event(lastSequence, template.topic(), template.partition(), template.offset(),
                    template.timestamp(), template.key(), template.headers(), template.payload(),
                    template.error(), template.rawBase64(), template.payloadTruncated()));
            while (history.size() > HISTORY_LIMIT) {
                history.removeFirst();
            }
            return lastSequence;
        } finally {
            lock.unlock();
        }
    }

    public PollResult poll(long sinceOffset) {
        long since = Math.max(0, sinceOffset);
        lock.lock();
        try {
            List<Event> out = new ArrayList<>();
            for (Event e : history) {
                if (e.sequence() > since) {
                    out.add(e);
                }
            }
            long oldestRetained = history.isEmpty() ? lastSequence + 1 : history.peekFirst().sequence();
            long missed = Math.max(0, oldestRetained - 1 - since);
            return new PollResult(id, List.copyOf(out), lastSequence, missed, status, lastSequence);
        } finally {
            lock.unlock();
        }
    }

    // ---- lifecycle plumbing used by the manager / listener ----------------------------------------

    void markListening() {
        if (status == Status.STARTING) {
            status = Status.LISTENING;
        }
    }

    void markCapReached() {
        if (status != Status.STOPPED) {
            status = Status.CAP_REACHED;
        }
    }

    void markStopped() {
        status = Status.STOPPED;
    }

    MessageListenerContainer container() {
        return container;
    }

    void container(MessageListenerContainer container) {
        this.container = container;
    }

    void cleanup(Runnable cleanup) {
        this.cleanup = cleanup;
    }

    void runCleanup() {
        cleanup.run();
    }

    /** True exactly once: guards teardown so unregister + stop run a single time per session. */
    boolean beginTeardown() {
        return tornDown.compareAndSet(false, true);
    }
}
