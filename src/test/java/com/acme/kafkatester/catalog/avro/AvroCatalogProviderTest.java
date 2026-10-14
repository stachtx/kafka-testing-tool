package com.acme.kafkatester.catalog.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.acme.kafkatester.catalog.DefaultTopics;

class AvroCatalogProviderTest {

    @Test
    void duplicateSimpleNamesAcrossNamespacesFailFast() {
        assertThat(new AvroCatalogProvider("classpath*:dup-schemas/a/*.avsc", DefaultTopics.empty())
                .resolve("Dup")).isPresent();

        assertThatThrownBy(() -> new AvroCatalogProvider("classpath*:dup-schemas/*/Dup.avsc", DefaultTopics.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate Avro event type name 'Dup'")
                .hasMessageContaining("team.a.Dup")
                .hasMessageContaining("team.b.Dup");
    }

    @Test
    void sidecarTopicWinsOverLegacyInSchemaProperty() {
        AvroCatalogProvider legacyOnly = new AvroCatalogProvider("classpath*:dup-schemas/c/*.avsc", DefaultTopics.empty());
        assertThat(legacyOnly.resolve("Solo").orElseThrow().defaultTopic()).isEqualTo("legacy.topic");

        AvroCatalogProvider sidecar = new AvroCatalogProvider("classpath*:dup-schemas/c/*.avsc",
                DefaultTopics.of(Map.of("Solo", "sidecar.topic")));
        assertThat(sidecar.resolve("Solo").orElseThrow().defaultTopic()).isEqualTo("sidecar.topic");
    }
}
