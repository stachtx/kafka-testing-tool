package com.acme.kafkatester.web.dto;

import java.util.List;

import com.acme.kafkatester.catalog.EventTypeDescriptor;
import com.acme.kafkatester.catalog.SchemaCatalog;

/** Response DTOs of {@code /api/events}. */
public final class EventDtos {

    private EventDtos() {
    }

    public record FieldDto(String name, String type, boolean nullable) {
    }

    /** One catalog entry. {@code format} is {@code AVRO} or {@code PROTOBUF}. */
    public record EventTypeDto(String name, String format, String defaultTopic, String description,
            List<FieldDto> fields) {

        public static EventTypeDto from(EventTypeDescriptor d) {
            return new EventTypeDto(d.name(), d.format(), d.defaultTopic(), d.description(),
                    d.fields().stream().map(f -> new FieldDto(f.name(), f.type(), f.nullable())).toList());
        }
    }

    public record EventTypePageDto(List<EventTypeDto> items, int offset, int limit, int total) {

        public static EventTypePageDto from(SchemaCatalog.Page<EventTypeDescriptor> page) {
            return new EventTypePageDto(page.items().stream().map(EventTypeDto::from).toList(), page.offset(),
                    page.limit(), page.total());
        }
    }

    /** Example payload; {@code payload} is JSON text the producer endpoints accept as-is. */
    public record ExampleDto(String eventType, String format, String payload) {
    }
}
