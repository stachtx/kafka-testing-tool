package com.acme.kafkatester.architecture.fixture;

import com.acme.kafkatester.registry.SchemaRegistrationService;

/** Deliberately violating fixture: injects SchemaRegistrationService outside the controller. */
public class RogueServiceInjector {

    private final SchemaRegistrationService service;

    public RogueServiceInjector(SchemaRegistrationService service) {
        this.service = service;
    }

    public SchemaRegistrationService service() {
        return service;
    }
}
