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
 * Verifies the Consumers screen script (Group 9) is served as a static resource.
 *
 * <h2>Manual verification checklist (Consumers screen)</h2>
 * Start the app (`./gradlew bootRun`, Kafka and Schema Registry env vars set), open http://localhost:8080/#/consumers
 * <ol>
 *   <li><b>Start LATEST:</b> enter topic {@code sandbox.ping}, Format AVRO, Offset LATEST, click "Start Consumer":
 *       a session card appears (format badge, topic, {@code group: kafka-tester-<id>}, {@code since: HH:MM:SS - LATEST},
 *       {@code events: N}, "Stop") and the event viewer header shows topic, group id and "polling 2s".</li>
 *   <li><b>Events arrive:</b> send a PingEvent from the Producer screen; within ~2s a row {@code #offset / partition N /
 *       time} appears at the top of the viewer and {@code events:} increments. Rows already shown are never duplicated.</li>
 *   <li><b>Expandable row:</b> click a row: key, headers and the pretty-printed decoded payload are shown; click again
 *       to collapse. An undecodable record shows the error plus a raw base64 note instead of a payload.</li>
 *   <li><b>Start EARLIEST with confirm:</b> select EARLIEST and click Start: a confirmation modal explains that the
 *       server-side cap applies (and names the cap if another EARLIEST session already reported it). Cancel starts
 *       nothing; Confirm starts the session and the card reads {@code EARLIEST (capped N)}. At the cap the card shows
 *       "auto-stopped at cap".</li>
 *   <li><b>Stop:</b> click "Stop": {@code DELETE /api/consumers/{id}} is sent (network tab), the card disappears. Stopping
 *       an already-stopped or auto-stopped session (404 CONSUMER_SESSION_NOT_FOUND) removes the card and shows a notice.</li>
 *   <li><b>Reload:</b> reload the page while a session runs: the card is rebuilt from {@code GET /api/consumers}; no
 *       session is orphaned. Switching to another screen and back behaves the same.</li>
 *   <li><b>Errors:</b> an invalid topic (spaces) is rejected inline; a failing start (e.g. Kafka down) shows the
 *       CONSUMER_START_FAILED error code, detail and correlation id.</li>
 *   <li><b>Polling pause:</b> hiding the tab stops requests; returning triggers an immediate poll with no lost events
 *       (cursor {@code sinceOffset = nextOffset}).</li>
 * </ol>
 */
@WebMvcTest(controllers = HealthController.class)
class ConsumersUiTest {

    @Autowired MockMvc mvc;

    @MockitoBean KafkaSecurityConfig securityConfig;
    @MockitoBean SchemaRegistryGateway gateway;

    @Test
    void consumersScriptIsServed() throws Exception {
        mvc.perform(get("/js/consumers.js"))
                .andExpect(status().isOk());
    }

    @Test
    void consumersScriptRegistersScreenAndUsesSessionEndpoints() throws Exception {
        mvc.perform(get("/js/consumers.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.allOf(
                        Matchers.containsString("registerScreen('consumers'"),
                        Matchers.containsString("/api/consumers"),
                        Matchers.containsString("sinceOffset"),
                        Matchers.containsString("CONSUMER_SESSION_NOT_FOUND"))));
    }

    @Test
    void indexLoadsConsumersScriptStaticallyAndAppHasNoDynamicLoader() throws Exception {
        mvc.perform(get("/index.html"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.containsString("<script src=\"js/consumers.js\"></script>")));
        mvc.perform(get("/js/app.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.not(Matchers.containsString("loadScript"))));
    }

    @Test
    void earliestConfirmationReadsConfiguredCapFromHealthAndHasNoDeadHelper() throws Exception {
        mvc.perform(get("/js/consumers.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.allOf(
                        Matchers.containsString("/api/kafka/health"),
                        Matchers.containsString("earliestCap"),
                        Matchers.containsString("payloadTruncated"),
                        Matchers.not(Matchers.containsString("shortGroup")))));
    }
}
