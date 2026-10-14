package com.acme.kafkatester.web.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.registry.SchemaRegistrationService;
import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.web.dto.InvalidRequestException;
import com.acme.kafkatester.web.dto.RegistryDtos.RegisterRequest;
import com.acme.kafkatester.web.dto.RegistryDtos.RegistrationPreviewDto;
import com.acme.kafkatester.web.dto.RegistryDtos.RegistrationResultDto;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

/**
 * Schema registration. Preview (read-only, via the gateway) and register (the only write, via the
 * service) are separate endpoints. This is the only class that injects {@link SchemaRegistrationService}.
 */
@RestController
@RequestMapping("/api/schemas")
@Tag(name = "Schema Registry")
public class SchemaRegistrationController {

    private final SchemaRegistryGateway gateway;
    private final SchemaRegistrationService registration;

    public SchemaRegistrationController(SchemaRegistryGateway gateway, SchemaRegistrationService registration) {
        this.gateway = gateway;
        this.registration = registration;
    }

    @GetMapping("/{eventType}/registration-preview")
    @Operation(summary = "Read-only preview: local vs registry schema and compatibility verdict")
    public RegistrationPreviewDto preview(@PathVariable String eventType,
            @RequestParam(required = false) String format, @RequestParam(required = false) String topic) {
        MessageFormat f = InvalidRequestException.optionalFormat(format);
        String t = topic == null || topic.isBlank() ? null : InvalidRequestException.topic(topic);
        return RegistrationPreviewDto.from(gateway.preview(eventType, f == null ? null : f.name(), t));
    }

    @PostMapping("/register")
    @Operation(summary = "Register the catalog schema under the topic's value subject")
    public RegistrationResultDto register(@Valid @RequestBody RegisterRequest request) {
        MessageFormat f = InvalidRequestException.optionalFormat(request.format());
        String t = request.topic() == null || request.topic().isBlank() ? null
                : InvalidRequestException.topic(request.topic());
        return RegistrationResultDto.from(
                registration.register(request.eventType().trim(), f == null ? null : f.name(), t));
    }
}
