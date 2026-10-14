package com.acme.kafkatester.web.rest;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.acme.kafkatester.producer.KafkaProducerService;
import com.acme.kafkatester.web.dto.InvalidRequestException;
import com.acme.kafkatester.web.dto.ProducerDtos.ProducerRequestDto;
import com.acme.kafkatester.web.dto.ProducerDtos.SendResponse;
import com.acme.kafkatester.web.dto.ProducerDtos.ValidationResponse;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

/** Validate (dry run) and send user-authored events. */
@RestController
@RequestMapping("/api/producer")
@Tag(name = "Producer")
public class ProducerController {

    private final KafkaProducerService producer;

    public ProducerController(KafkaProducerService producer) {
        this.producer = producer;
    }

    @PostMapping("/validate")
    @Operation(summary = "Validate a payload against the event schema without sending")
    public ValidationResponse validate(@Valid @RequestBody ProducerRequestDto request) {
        return ValidationResponse.from(producer.validate(prepared(request)));
    }

    @PostMapping("/send")
    @Operation(summary = "Serialize and send an event to Kafka")
    public SendResponse send(@Valid @RequestBody ProducerRequestDto request) {
        return SendResponse.from(producer.send(prepared(request)));
    }

    private static KafkaProducerService.ProducerRequest prepared(ProducerRequestDto request) {
        InvalidRequestException.topic(request.topic());
        InvalidRequestException.optionalFormat(request.format());
        return request.toDomain();
    }
}
