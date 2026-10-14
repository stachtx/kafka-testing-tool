package com.acme.kafkatester.architecture.fixture.producer;

import com.acme.kafkatester.registry.SchemaRegistrationService;

/** Deliberately violating fixture: a "producer" class referencing SchemaRegistrationService. */
public class RegistrationUsingProducer {

    private final SchemaRegistrationService service;

    public RegistrationUsingProducer(SchemaRegistrationService service) {
        this.service = service;
    }

    public Object service() {
        return service;
    }
}
