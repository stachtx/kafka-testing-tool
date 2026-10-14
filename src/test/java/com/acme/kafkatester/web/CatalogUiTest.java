package com.acme.kafkatester.web;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;
import com.acme.kafkatester.web.rest.HealthController;

/**
 * Verifies the Event Catalog screen script (Group 10) is served and wired to the expected endpoints and event bus.
 *
 * <h2>Manual verification checklist (Event Catalog screen)</h2>
 * Start the app (`./gradlew bootRun`) and open http://localhost:8080/#/catalog
 * <ol>
 *   <li><b>Grid:</b> heading "Event Catalog" with subtitle "2 event types discovered"; two cards, both named
 *       {@code PingEvent}, one AVRO badge and one PROTOBUF badge, each with the "sample" tag, a description and a
 *       "Register / Update Schema" button.</li>
 *   <li><b>Select:</b> click a card: it is highlighted and the detail panel shows the event name + format badge, a
 *       Fields table (name, type, nullable marker for the nullable field) and the "Example Payload" pretty-printed
 *       JSON. Selecting the other PingEvent card shows the payload for that format (format is always passed).</li>
 *   <li><b>Register button:</b> click "Register / Update Schema": the selection does NOT change and a
 *       {@code register-schema} document event with {@code {eventType, format}} is dispatched (the registration modal,
 *       Group 11, opens from it).</li>
 *   <li><b>Empty catalog:</b> with no providers, "0 event types discovered" and an explanatory empty state.</li>
 *   <li><b>Errors:</b> a failing {@code GET /api/events} shows a ProblemDetail panel with a Retry button; an
 *       {@code UNKNOWN_EVENT_TYPE} on the detail/example call shows a notice inside the detail panel and the catalog
 *       is reloaded.</li>
 *   <li><b>Pagination:</b> with more than one page of event types, all pages are loaded and the count equals
 *       {@code total}.</li>
 * </ol>
 */
@WebMvcTest(controllers = HealthController.class)
class CatalogUiTest {

    @Autowired MockMvc mvc;

    @MockitoBean KafkaSecurityConfig securityConfig;
    @MockitoBean SchemaRegistryGateway gateway;

    @Test
    void catalogScriptIsServed() throws Exception {
        mvc.perform(get("/js/catalog.js"))
                .andExpect(status().isOk());
    }

    @Test
    void catalogScriptRegistersScreenAndUsesEventEndpoints() throws Exception {
        mvc.perform(get("/js/catalog.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.allOf(
                        Matchers.containsString("registerScreen('catalog'"),
                        Matchers.containsString("/api/events"),
                        Matchers.containsString("/example"),
                        Matchers.containsString("UNKNOWN_EVENT_TYPE"),
                        Matchers.containsString("event types discovered"))));
    }

    @Test
    void catalogDispatchesRegisterEventAndNeverCallsRegistrationApi() throws Exception {
        mvc.perform(get("/js/catalog.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.allOf(
                        Matchers.containsString("register-schema"),
                        Matchers.containsString("stopPropagation"),
                        Matchers.not(Matchers.containsString("/api/schemas/register")))));
    }

    @Test
    void indexLoadsCatalogScriptStaticallyAndAppHasNoDynamicLoader() throws Exception {
        mvc.perform(get("/index.html"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("<script src=\"js/catalog.js\"></script>")));
        mvc.perform(get("/js/app.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.not(Matchers.containsString("loadScript"))));
    }
}
