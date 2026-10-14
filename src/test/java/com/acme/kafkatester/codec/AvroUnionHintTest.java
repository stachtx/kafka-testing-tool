package com.acme.kafkatester.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import com.acme.kafkatester.catalog.avro.AvroCatalogProvider;

/** Avro union UX: actionable hints on the classic Avro-JSON pitfalls. */
class AvroUnionHintTest {

    private final AvroSchemaCodec codec = new AvroSchemaCodec();
    private final Schema schema = new AvroCatalogProvider().schema("PingEvent").orElseThrow();

    @Test
    void unwrappedUnionValueGetsWrapperHint() {
        String json = "{\"id\":\"1\",\"message\":\"m\",\"sequence\":1,\"sentAt\":2,\"note\":\"plain\"}";

        assertThatThrownBy(() -> codec.encode(schema, json))
                .isInstanceOf(InvalidAvroPayloadException.class)
                .hasMessageContaining("Hint")
                .hasMessageContaining("{\"string\":\"x\"}");
    }

    @Test
    void missingDefaultedFieldGetsPresenceHint() {
        String json = "{\"id\":\"1\",\"message\":\"m\",\"sequence\":1,\"sentAt\":2}";

        assertThatThrownBy(() -> codec.encode(schema, json))
                .isInstanceOf(InvalidAvroPayloadException.class)
                .hasMessageContaining("Hint")
                .hasMessageContaining("present");
    }

    @Test
    void wrappedUnionValueAndNullStillEncode() {
        String wrapped = "{\"id\":\"1\",\"message\":\"m\",\"sequence\":1,\"sentAt\":2,\"note\":{\"string\":\"x\"}}";
        String nul = "{\"id\":\"1\",\"message\":\"m\",\"sequence\":1,\"sentAt\":2,\"note\":null}";

        assertThat(codec.encode(schema, wrapped)).isNotNull();
        assertThat(codec.encode(schema, nul)).isNotNull();
    }
}
