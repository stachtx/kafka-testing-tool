package com.acme.kafkatester.web.rest;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.acme.kafkatester.consumer.ConsumerSessionManager;
import com.acme.kafkatester.web.dto.ConsumerDtos.ConsumerSessionDto;
import com.acme.kafkatester.web.dto.ConsumerDtos.PollResponse;
import com.acme.kafkatester.web.dto.ConsumerDtos.StartConsumerRequest;
import com.acme.kafkatester.web.dto.InvalidRequestException;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

/** Dynamic consumer sessions: start, poll with a cursor, stop. */
@RestController
@RequestMapping("/api/consumers")
@Tag(name = "Consumers")
public class ConsumersController {

    private final ConsumerSessionManager sessions;

    public ConsumersController(ConsumerSessionManager sessions) {
        this.sessions = sessions;
    }

    @GetMapping
    @Operation(summary = "List consumer sessions")
    public List<ConsumerSessionDto> list() {
        return sessions.listSessions().stream().map(ConsumerSessionDto::from).toList();
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(summary = "Start a consumer session")
    public ConsumerSessionDto start(@Valid @RequestBody StartConsumerRequest request) {
        String topic = InvalidRequestException.topic(request.topic());
        var format = InvalidRequestException.format(request.format());
        var offsetMode = InvalidRequestException.offsetMode(request.offsetMode());
        return ConsumerSessionDto.from(sessions.startSession(topic, format, offsetMode));
    }

    @GetMapping("/{id}/events")
    @Operation(summary = "Poll events newer than sinceOffset (the previous nextOffset; 0 for the first poll)")
    public PollResponse poll(@PathVariable String id, @RequestParam(defaultValue = "0") long sinceOffset) {
        return PollResponse.from(sessions.poll(id, Math.max(0, sinceOffset)));
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @Operation(summary = "Stop a consumer session")
    public void stop(@PathVariable String id) {
        sessions.stopSession(id);
    }
}
