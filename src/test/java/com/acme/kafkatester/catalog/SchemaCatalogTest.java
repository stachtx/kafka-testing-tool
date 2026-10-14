package com.acme.kafkatester.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.acme.kafkatester.catalog.avro.AvroCatalogProvider;
import com.acme.kafkatester.catalog.protobuf.ProtobufCatalogProvider;

class SchemaCatalogTest {

    private final SchemaCatalog catalog =
            new SchemaCatalog(List.of(new AvroCatalogProvider(), new ProtobufCatalogProvider()));

    @Test
    void avroProviderLoadsPingEventFromAvsc() {
        EventTypeDescriptor d = new AvroCatalogProvider().resolve("PingEvent").orElseThrow();
        assertThat(d.format()).isEqualTo(EventTypeDescriptor.FORMAT_AVRO);
        assertThat(d.defaultTopic()).isEqualTo("sandbox.ping");
        assertThat(d.fields()).extracting(EventTypeDescriptor.Field::name)
                .containsExactly("id", "message", "sequence", "sentAt", "note");
        assertThat(d.fields()).filteredOn(EventTypeDescriptor.Field::nullable)
                .extracting(EventTypeDescriptor.Field::name).containsExactly("note");
        assertThat(new AvroCatalogProvider().schema("PingEvent")).isPresent();
    }

    @Test
    void protobufProviderLoadsPingEventFromDescriptorSet() {
        ProtobufCatalogProvider provider = new ProtobufCatalogProvider();
        EventTypeDescriptor d = provider.resolve("PingEvent").orElseThrow();
        assertThat(d.format()).isEqualTo(EventTypeDescriptor.FORMAT_PROTOBUF);
        assertThat(d.defaultTopic()).isEqualTo("sandbox.ping");
        assertThat(d.fields()).extracting(EventTypeDescriptor.Field::name)
                .containsExactly("id", "message", "sequence", "sent_at", "note");
        assertThat(provider.descriptor("PingEvent")).isPresent();
        // well-known imports bundled in the descriptor set are not catalogued
        assertThat(provider.listEventTypes()).extracting(EventTypeDescriptor::name)
                .containsExactly("PingEvent");
    }

    @Test
    void catalogListsBothFormats() {
        assertThat(catalog.listAll()).extracting(EventTypeDescriptor::format)
                .containsExactly("AVRO", "PROTOBUF");
    }

    @Test
    void resolvesByNameAndByNameAndFormat() {
        assertThat(catalog.resolve("PingEvent").format()).isEqualTo("AVRO");
        assertThat(catalog.resolve("PingEvent", "PROTOBUF").format()).isEqualTo("PROTOBUF");
    }

    @Test
    void unknownTypeThrows() {
        assertThatThrownBy(() -> catalog.resolve("Nope"))
                .isInstanceOf(UnknownEventTypeException.class)
                .hasMessageContaining("Nope");
        assertThatThrownBy(() -> catalog.resolve("PingEvent", "XML"))
                .isInstanceOf(UnknownEventTypeException.class);
    }

    @Test
    void paginationAppliesOffsetAndLimit() {
        SchemaCatalog.Page<EventTypeDescriptor> first = catalog.list(0, 1);
        assertThat(first.items()).hasSize(1);
        assertThat(first.total()).isEqualTo(2);
        SchemaCatalog.Page<EventTypeDescriptor> second = catalog.list(1, 10);
        assertThat(second.items()).extracting(EventTypeDescriptor::format).containsExactly("PROTOBUF");
        assertThat(catalog.list(5, 10).items()).isEmpty();
    }

    @Test
    void generateExampleOnUnknownTypeThrows() {
        assertThatThrownBy(() -> catalog.generateExample("Nope"))
                .isInstanceOf(UnknownEventTypeException.class);
    }
}
