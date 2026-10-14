package com.acme.kafkatester.catalog;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Sidecar convention for suggested topics: {@code classpath*:schemas/default-topics.properties},
 * one {@code <EventName>=<topic>} line per event type, applying to every format. Keeping this out of
 * the schemas themselves means tool-private metadata is never registered into the shared registry.
 */
public final class DefaultTopics {

    public static final String DEFAULT_LOCATION = "classpath*:schemas/default-topics.properties";

    private static final DefaultTopics EMPTY = new DefaultTopics(Map.of());

    private final Map<String, String> topics;

    private DefaultTopics(Map<String, String> topics) {
        this.topics = topics;
    }

    public static DefaultTopics empty() {
        return EMPTY;
    }

    public static DefaultTopics of(Map<String, String> topics) {
        return new DefaultTopics(Map.copyOf(topics));
    }

    public static DefaultTopics load() {
        return load(DEFAULT_LOCATION);
    }

    public static DefaultTopics load(String locationPattern) {
        Map<String, String> merged = new TreeMap<>();
        try {
            for (Resource r : new PathMatchingResourcePatternResolver().getResources(locationPattern)) {
                Properties p = new Properties();
                try (InputStream in = r.getInputStream()) {
                    p.load(in);
                }
                p.stringPropertyNames().forEach(k -> {
                    String v = p.getProperty(k).trim();
                    if (!v.isEmpty()) {
                        merged.put(k.trim(), v);
                    }
                });
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load default topics from " + locationPattern, e);
        }
        return new DefaultTopics(Map.copyOf(merged));
    }

    /** Suggested topic for an event type's simple name, if declared. */
    public Optional<String> forEvent(String eventName) {
        return Optional.ofNullable(topics.get(eventName));
    }
}
