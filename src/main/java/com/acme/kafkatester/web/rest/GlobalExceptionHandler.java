package com.acme.kafkatester.web.rest;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.acme.kafkatester.catalog.UnknownEventTypeException;
import com.acme.kafkatester.codec.InvalidPayloadException;
import com.acme.kafkatester.consumer.ConsumerLimitReachedException;
import com.acme.kafkatester.consumer.ConsumerSessionNotFoundException;
import com.acme.kafkatester.consumer.ConsumerStartException;
import com.acme.kafkatester.producer.KafkaSendFailedException;
import com.acme.kafkatester.registry.SchemaRegistryException;
import com.acme.kafkatester.web.CorrelationIdFilter;
import com.acme.kafkatester.web.dto.InvalidRequestException;
import com.acme.kafkatester.web.dto.InvalidRequestException.FieldError;

/**
 * Every error leaves the API as an RFC 7807 {@link ProblemDetail} with a taxonomy {@code errorCode}, a
 * {@code correlationId} (also the {@code X-Correlation-Id} header and MDC) and optional {@code errors[]}.
 * Full detail (including causes and stack traces) is logged server-side only; response text is
 * sanitized and never echoes causes.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    public static final String UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE";
    public static final String INVALID_JSON = "INVALID_JSON";
    public static final String SERIALIZATION_FAILED = "SERIALIZATION_FAILED";
    public static final String SCHEMA_REGISTRY_ERROR = "SCHEMA_REGISTRY_ERROR";
    public static final String VALIDATION_ERROR = "VALIDATION_ERROR";
    public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(UnknownEventTypeException.class)
    ResponseEntity<Object> unknownEventType(UnknownEventTypeException e) {
        log.warn("Unknown event type: {}", e.getMessage());
        return problem(HttpStatus.NOT_FOUND, UNKNOWN_EVENT_TYPE, e.getMessage(), null);
    }

    @ExceptionHandler(InvalidPayloadException.class)
    ResponseEntity<Object> invalidPayload(InvalidPayloadException e) {
        log.warn("{}: {}", e.errorCode(), e.getMessage());
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, e.errorCode(), e.getMessage(), null);
    }

    @ExceptionHandler(SerializationException.class)
    ResponseEntity<Object> serialization(SerializationException e) {
        log.warn("Serialization failed", e);
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, SERIALIZATION_FAILED,
                "The event could not be serialized with its schema", null);
    }

    @ExceptionHandler(KafkaSendFailedException.class)
    ResponseEntity<Object> sendFailed(KafkaSendFailedException e) {
        log.error("Kafka send failed", e);
        return problem(HttpStatus.BAD_GATEWAY, e.errorCode(), "Sending to Kafka failed", null);
    }

    @ExceptionHandler(ConsumerStartException.class)
    ResponseEntity<Object> consumerStart(ConsumerStartException e) {
        log.error("Consumer start failed", e);
        return problem(HttpStatus.BAD_GATEWAY, ConsumerStartException.ERROR_CODE, safe(e.getMessage()), null);
    }

    @ExceptionHandler(ConsumerSessionNotFoundException.class)
    ResponseEntity<Object> sessionNotFound(ConsumerSessionNotFoundException e) {
        log.warn("{}", e.getMessage());
        return problem(HttpStatus.NOT_FOUND, ConsumerSessionNotFoundException.ERROR_CODE, e.getMessage(), null);
    }

    @ExceptionHandler(ConsumerLimitReachedException.class)
    ResponseEntity<Object> consumerLimit(ConsumerLimitReachedException e) {
        log.warn("Consumer session limit reached ({})", e.maxSessions());
        return problem(HttpStatus.TOO_MANY_REQUESTS, ConsumerLimitReachedException.ERROR_CODE, e.getMessage(), null);
    }

    @ExceptionHandler(SchemaRegistryException.class)
    ResponseEntity<Object> registry(SchemaRegistryException e) {
        HttpStatus status = e.httpStatus() == 409 ? HttpStatus.CONFLICT : HttpStatus.BAD_GATEWAY;
        if (status == HttpStatus.CONFLICT) {
            log.warn("Schema Registry conflict: {}", e.getMessage()); // expected (pre-send guard / incompatible), no trace
        } else {
            log.error("Schema Registry error", e);
        }
        return problem(status, SCHEMA_REGISTRY_ERROR, e.getMessage(), null); // message is sanitized at construction
    }

    @ExceptionHandler(InvalidRequestException.class)
    ResponseEntity<Object> invalidRequest(InvalidRequestException e) {
        log.warn("Invalid request: {}", e.getMessage());
        return problem(HttpStatus.BAD_REQUEST, VALIDATION_ERROR, e.getMessage(), e.errors());
    }

    // No blanket IllegalArgumentException handler: request validation is done by InvalidRequestException /
    // bean validation. An IAE that escapes is a server-side defect and falls through to INTERNAL_ERROR below
    // (fixed text, details logged only), so internal messages are never echoed to clients.

    @ExceptionHandler(Exception.class)
    ResponseEntity<Object> unexpected(Exception e) {
        log.error("Unexpected error", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, INTERNAL_ERROR,
                "An unexpected error occurred. Quote the correlation id when reporting it.", null);
    }

    // ---- framework exceptions -----------------------------------------------------------------

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        log.warn("Unreadable request body: {}", ex.getMessage());
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Request body is missing or is not valid JSON");
        pd.setTitle("Invalid JSON");
        pd.setProperty("errorCode", INVALID_JSON);
        return handleExceptionInternal(ex, pd, headers, HttpStatus.BAD_REQUEST, request);
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        List<FieldError> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(f -> new FieldError(f.getField(), f.getDefaultMessage())).toList();
        log.warn("Request validation failed: {}", errors);
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Request validation failed");
        pd.setTitle("Validation error");
        pd.setProperty("errorCode", VALIDATION_ERROR);
        pd.setProperty("errors", errors);
        return handleExceptionInternal(ex, pd, headers, HttpStatus.BAD_REQUEST, request);
    }

    /** Funnel for framework-produced responses: guarantees errorCode + correlationId on every body. */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
            HttpStatusCode statusCode, WebRequest request) {
        ProblemDetail pd = body instanceof ProblemDetail p ? p : ProblemDetail.forStatus(statusCode);
        enrich(pd, statusCode.value() == 400 ? VALIDATION_ERROR : "HTTP_" + statusCode.value());
        if (statusCode.is5xxServerError()) {
            log.error("Framework error", ex);
        }
        return super.handleExceptionInternal(ex, pd, headers, statusCode, request);
    }

    // ---- helpers ------------------------------------------------------------------------------

    private ResponseEntity<Object> problem(HttpStatus status, String errorCode, String detail, List<FieldError> errors) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        pd.setTitle(status.getReasonPhrase());
        pd.setType(URI.create("about:blank"));
        pd.setProperty("errorCode", errorCode);
        if (errors != null && !errors.isEmpty()) {
            pd.setProperty("errors", errors);
        }
        enrich(pd, errorCode);
        return ResponseEntity.status(status).body(pd);
    }

    private static void enrich(ProblemDetail pd, String defaultErrorCode) {
        if (pd.getProperties() == null || !pd.getProperties().containsKey("errorCode")) {
            pd.setProperty("errorCode", defaultErrorCode);
        }
        String id = MDC.get(CorrelationIdFilter.MDC_KEY);
        pd.setProperty("correlationId", id != null ? id : UUID.randomUUID().toString());
    }

    private static String safe(String message) {
        return SchemaRegistryException.sanitize(message);
    }
}
