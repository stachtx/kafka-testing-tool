package com.acme.kafkatester.registry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SchemaRegistrySanitizeTest {

    @Test
    void masksJsonQuotedAndApiKeyForms() {
        assertThat(SchemaRegistryException.sanitize("{\"password\":\"hunter2\",\"x\":1}")).doesNotContain("hunter2");
        assertThat(SchemaRegistryException.sanitize("{\"password\": \"two words\"}")).doesNotContain("two words");
        assertThat(SchemaRegistryException.sanitize("api_key=abc123")).doesNotContain("abc123");
        assertThat(SchemaRegistryException.sanitize("api-key: abc123")).doesNotContain("abc123");
        assertThat(SchemaRegistryException.sanitize("X-ApiKey=abc123")).doesNotContain("abc123");
        assertThat(SchemaRegistryException.sanitize("password=hunter2")).doesNotContain("hunter2");
    }

    @Test
    void masksTokenLookingBasicAndBearerValues() {
        assertThat(SchemaRegistryException.sanitize("Authorization: Basic dXNlcjpwYXNzd29yZA==")).doesNotContain("dXNlcjpw");
        assertThat(SchemaRegistryException.sanitize("Bearer eyJhbGciOi.eyJzdWIi.sig-na_ture")).doesNotContain("eyJhbGciOi");
    }

    @Test
    void keepsProseAboutBasicAuthentication() {
        String prose = "basic authentication failed; bearer authentication is required";
        assertThat(SchemaRegistryException.sanitize(prose)).isEqualTo(prose);
    }
}
