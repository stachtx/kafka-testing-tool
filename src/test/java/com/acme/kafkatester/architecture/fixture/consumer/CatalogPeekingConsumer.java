package com.acme.kafkatester.architecture.fixture.consumer;

import com.acme.kafkatester.catalog.SchemaCatalog;

/** Deliberately violating fixture: a "consumer" class depending on catalog/. */
public class CatalogPeekingConsumer {

    private final SchemaCatalog catalog;

    public CatalogPeekingConsumer(SchemaCatalog catalog) {
        this.catalog = catalog;
    }

    public int knownTypes() {
        return catalog.listAll().size();
    }
}
