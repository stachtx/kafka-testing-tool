package com.acme.kafkatester.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/** Full-context smoke test: springdoc serves the API docs and Swagger UI; no format path variants exist. */
@SpringBootTest
@AutoConfigureMockMvc
class OpenApiSmokeTest {

    @Autowired MockMvc mvc;

    @Test
    void apiDocsListEndpointsWithoutFormatPathVariants() throws Exception {
        MvcResult result = mvc.perform(get("/v3/api-docs")).andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(200);
        String body = result.getResponse().getContentAsString();
        assertThat(body).contains("/api/events", "/api/producer/send", "/api/consumers", "/api/schemas/register",
                "/api/kafka/health", "Kafka Testing Tool API");
        assertThat(body).doesNotContain("/avro").doesNotContain("/protobuf");
    }

    @Test
    void swaggerUiIsServed() throws Exception {
        int redirect = mvc.perform(get("/swagger-ui.html")).andReturn().getResponse().getStatus();
        assertThat(redirect).isIn(200, 302);
        MvcResult index = mvc.perform(get("/swagger-ui/index.html")).andReturn();
        assertThat(index.getResponse().getStatus()).isEqualTo(200);
    }
}
