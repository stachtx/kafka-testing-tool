package com.acme.kafkatester;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(KafkaTesterProperties.class)
public class KafkaTesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTesterApplication.class, args);
    }
}
