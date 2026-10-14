package com.acme.kafkatester.web.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.acme.kafkatester.catalog.EventTypeDescriptor;
import com.acme.kafkatester.catalog.SchemaCatalog;
import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.web.dto.EventDtos.EventTypeDto;
import com.acme.kafkatester.web.dto.EventDtos.EventTypePageDto;
import com.acme.kafkatester.web.dto.EventDtos.ExampleDto;
import com.acme.kafkatester.web.dto.InvalidRequestException;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/** Event-type catalog. One shape for every format; {@code format} is an optional disambiguating parameter. */
@RestController
@RequestMapping("/api/events")
@Tag(name = "Events")
public class EventsController {

    private final SchemaCatalog catalog;

    public EventsController(SchemaCatalog catalog) {
        this.catalog = catalog;
    }

    @GetMapping
    @Operation(summary = "List event types (paginated)")
    public EventTypePageDto list(@RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "50") int limit) {
        return EventTypePageDto.from(catalog.list(offset, limit));
    }

    @GetMapping("/{eventType}")
    @Operation(summary = "Describe an event type; pass format when the name exists in several formats")
    public EventTypeDto get(@PathVariable String eventType, @RequestParam(required = false) String format) {
        return EventTypeDto.from(resolve(eventType, format));
    }

    @GetMapping("/{eventType}/example")
    @Operation(summary = "Generate an example JSON payload")
    public ExampleDto example(@PathVariable String eventType, @RequestParam(required = false) String format) {
        EventTypeDescriptor d = resolve(eventType, format);
        return new ExampleDto(d.name(), d.format(), catalog.generateExample(d.name(), d.format()));
    }

    private EventTypeDescriptor resolve(String eventType, String format) {
        MessageFormat parsed = InvalidRequestException.optionalFormat(format);
        return parsed == null ? catalog.resolve(eventType) : catalog.resolve(eventType, parsed.name());
    }
}
