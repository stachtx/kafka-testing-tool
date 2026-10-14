package com.acme.kafkatester.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;

/** springdoc metadata. Docs at {@code /v3/api-docs}, UI at {@code /swagger-ui.html}. */
@Configuration(proxyBeanMethods = false)
public class OpenApiConfig {

    @Bean
    OpenAPI kafkaTesterOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Kafka Testing Tool API")
                .version("v1")
                .description("Produce and consume Avro/Protobuf events on a DEV Kafka cluster. "
                        + "One endpoint per concern; format is a field, not a path variant. "
                        + "Errors are RFC 7807 problem details with an errorCode and correlationId."));
    }
}
