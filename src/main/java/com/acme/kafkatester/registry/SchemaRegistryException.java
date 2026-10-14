package com.acme.kafkatester.registry;

import java.util.regex.Pattern;

/**
 * A Schema Registry read or write failed (maps to {@code SCHEMA_REGISTRY_ERROR}). The message is
 * sanitized: URL userinfo, basic-auth/bearer values and {@code key=value} secrets are masked, so it
 * is safe to return to API clients. The (unsanitized) cause is retained for server-side logging only.
 */
public class SchemaRegistryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private static final Pattern URL_USERINFO = Pattern.compile("(?i)([a-z][a-z0-9+.-]*://)[^/\\s@]+@");
    /** Basic/Bearer followed by a token-looking value (>= 8 chars with a digit/symbol or an inner capital), so prose like "basic authentication" is kept. */
    private static final Pattern AUTH_HEADER = Pattern.compile(
            "(?i:\\b(basic|bearer))\\s+(?=[A-Za-z0-9._~+/=-]{8,})(?=[A-Za-z0-9._~+/=-]*(?:[0-9._~+/=-]|[a-z][A-Z]))[A-Za-z0-9._~+/=-]+");
    /** key=value / key: value / JSON "key":"value" for secret-like keys (password, secret, token, api key, ...). */
    private static final Pattern KEY_VALUE = Pattern.compile(
            "(?i)([\"']?[\\w.-]*(?:password|secret|token|credential|user\\.info|jaas\\.config|api[_-]?key)[\\w.-]*[\"']?)"
                    + "\\s*[=:]\\s*(?:\"[^\"]*\"|'[^']*'|\\S+)");

    private final int httpStatus;

    public SchemaRegistryException(String message) {
        this(message, null, -1);
    }

    public SchemaRegistryException(String message, Throwable cause) {
        this(message, cause, -1);
    }

    public SchemaRegistryException(String message, Throwable cause, int httpStatus) {
        super(sanitize(message), cause);
        this.httpStatus = httpStatus;
    }

    /** Builds {@code "Schema Registry <operation> failed: <sanitized cause message>"}. */
    static SchemaRegistryException failed(String operation, Throwable cause) {
        int status = cause instanceof io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException r
                ? r.getStatus() : -1;
        String detail = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        return new SchemaRegistryException("Schema Registry " + operation + " failed: " + detail, cause, status);
    }

    /** HTTP status reported by the registry, or -1 when unknown/not applicable. */
    public int httpStatus() {
        return httpStatus;
    }

    /** Masks credentials in free text. Never returns {@code null}. */
    public static String sanitize(String text) {
        if (text == null) {
            return "";
        }
        String s = URL_USERINFO.matcher(text).replaceAll("$1***@");
        s = AUTH_HEADER.matcher(s).replaceAll("$1 ******");
        s = KEY_VALUE.matcher(s).replaceAll(m -> java.util.regex.Matcher.quoteReplacement(m.group(1) + "=******"));
        return s;
    }
}
