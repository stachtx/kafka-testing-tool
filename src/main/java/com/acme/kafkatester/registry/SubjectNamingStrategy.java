package com.acme.kafkatester.registry;

/** Strategy mapping a topic (and event type) to a registry subject. Default: {@code <topic>-value}. */
@FunctionalInterface
public interface SubjectNamingStrategy {

    String subjectFor(String topic, String eventType);

    /** Confluent TopicNameStrategy for values: {@code <topic>-value}. */
    static SubjectNamingStrategy topicValue() {
        return (topic, eventType) -> topic + "-value";
    }
}
