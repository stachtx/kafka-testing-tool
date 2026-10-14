package com.acme.kafkatester.catalog.protobuf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.acme.kafkatester.catalog.DefaultTopics;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;

class ProtobufCatalogProviderTest {

    private static FileDescriptorProto.Builder file(String name, String pkg, String message) {
        return FileDescriptorProto.newBuilder().setName(name).setPackage(pkg)
                .addMessageType(DescriptorProto.newBuilder().setName(message));
    }

    @Test
    void duplicateSimpleMessageNamesFailFast() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(file("a.proto", "team.a", "Dup"))
                .addFile(file("b.proto", "team.b", "Dup"))
                .build();

        assertThatThrownBy(() -> new ProtobufCatalogProvider(set, DefaultTopics.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate Protobuf event type name 'Dup'")
                .hasMessageContaining("team.a.Dup")
                .hasMessageContaining("team.b.Dup");
    }

    @Test
    void missingDescriptorDependencyFailsClearlyInsteadOfBeingDropped() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(file("a.proto", "team.a", "A").addDependency("missing/other.proto"))
                .build();

        assertThatThrownBy(() -> new ProtobufCatalogProvider(set, DefaultTopics.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("a.proto")
                .hasMessageContaining("missing/other.proto")
                .hasMessageContaining("missing from the descriptor set");
    }

    @Test
    void defaultTopicComesFromSidecarAndShippedDescriptorHasNoToolOption() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(file("a.proto", "team.a", "A")).build();

        ProtobufCatalogProvider provider = new ProtobufCatalogProvider(set, DefaultTopics.of(Map.of("A", "topic.a")));

        assertThat(provider.resolve("A").orElseThrow().defaultTopic()).isEqualTo("topic.a");
        assertThat(new ProtobufCatalogProvider().resolve("PingEvent").orElseThrow().defaultTopic())
                .isEqualTo("sandbox.ping");
        assertThat(new ProtobufCatalogProvider().descriptor("PingEvent").orElseThrow()
                .getFile().getDependencies()).isEmpty();
    }
}
