package com.acme.kafkatester.web;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;
import com.acme.kafkatester.web.rest.HealthController;

/**
 * Verifies the static UI shell (Group 8) is served by Spring Boot with no build step.
 *
 * <h2>Manual verification checklist (Producer screen, sample PingEvent)</h2>
 * Start the app (`./gradlew bootRun`, with Kafka and Schema Registry env vars set) and open http://localhost:8080/
 * <ol>
 *   <li><b>Shell:</b> brand "Kafka Tester"; nav links Producer / Consumers / Event Catalog; Producer is active and the
 *       URL becomes {@code #/producer}. Clicking Consumers / Event Catalog shows a "coming soon" placeholder until
 *       Groups 9-11 are present, and the nav highlight follows the hash.</li>
 *   <li><b>Event types:</b> Event Type dropdown lists "PingEvent (sample)". Switching Format AVRO / PROTOBUF re-filters
 *       the dropdown; PingEvent exists in both formats.</li>
 *   <li><b>Load Example:</b> choose PingEvent, click "Load Example": the Payload textarea is filled with the
 *       generated example; empty Topic is defaulted to {@code sandbox.ping}.</li>
 *   <li><b>Send success:</b> click "Send Event": the green panel shows "Sent successfully" with Topic, Partition,
 *       Offset and Timestamp.</li>
 *   <li><b>Validation error:</b> type {@code {"id": } } (invalid JSON) and click Send or Validate: an inline JSON parse
 *       error appears and no request is sent (check the network tab). Set a payload that violates the schema (for
 *       example a wrong field type): the red panel shows errorCode (INVALID_AVRO_PAYLOAD / INVALID_PROTOBUF_PAYLOAD),
 *       detail and correlation id.</li>
 *   <li><b>Validate:</b> "Validate" (left of Send Event) shows an inline "Payload is valid" result and sends nothing.</li>
 *   <li><b>Headers:</b> "+ Add header" appends a key/value row; the x button removes it; non-empty rows are sent as
 *       the {@code headers} object.</li>
 *   <li><b>Polling helper:</b> in the console, {@code ApiClient.startPolling(fn, 2000)} calls fn immediately, then
 *       every 2s; hiding the tab pauses it and returning to the tab triggers an immediate call.</li>
 * </ol>
 */
@WebMvcTest(controllers = HealthController.class)
class StaticUiTest {

    @Autowired MockMvc mvc;

    @MockitoBean KafkaSecurityConfig securityConfig;
    @MockitoBean SchemaRegistryGateway gateway;

    @Test
    void rootServesIndexHtmlShell() throws Exception {
        // The welcome page mapping forwards "/" to index.html (MockMvc does not follow forwards).
        mvc.perform(get("/"))
                .andExpect(status().isOk())
                .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl("index.html"));
        mvc.perform(get("/index.html"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Kafka Tester")));
    }

    @Test
    void indexHtmlLoadsSharedScriptsAndStylesheet() throws Exception {
        mvc.perform(get("/index.html"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.allOf(
                        org.hamcrest.Matchers.containsString("css/app.css"),
                        org.hamcrest.Matchers.containsString("js/apiClient.js"),
                        org.hamcrest.Matchers.containsString("js/app.js"),
                        org.hamcrest.Matchers.containsString("js/producer.js"))));
    }

    @Test
    void indexHasStaticScriptTagsForEveryScreenAndNoInlineCodeWithStrictCsp() throws Exception {
        String html = mvc.perform(get("/index.html")).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        org.assertj.core.api.Assertions.assertThat(html)
                .contains("<script src=\"js/producer.js\"></script>")
                .contains("<script src=\"js/consumers.js\"></script>")
                .contains("<script src=\"js/catalog.js\"></script>")
                .contains("<script src=\"js/registration.js\"></script>")
                .contains("Content-Security-Policy")
                .contains("default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:")
                .doesNotContain("unsafe-inline")
                .doesNotContainPattern("<script(?![^>]*\\bsrc=)[^>]*>")
                .doesNotContainPattern("<style\\b")
                .doesNotContainPattern("\\sstyle=");
    }

    @Test
    void scriptsDoNotSetInlineStyleAttributesWhichStrictStyleSrcWouldBlock() throws Exception {
        for (String script : new String[] { "app.js", "producer.js", "consumers.js", "catalog.js", "registration.js" }) {
            String js = mvc.perform(get("/js/" + script)).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            org.assertj.core.api.Assertions.assertThat(js).as(script)
                    .doesNotContainPattern("\\bstyle\\s*:\\s*'")
                    .doesNotContain("innerHTML").doesNotContain("eval(");
        }
    }

    @Test
    void producerShowsAvroUnionTipForInvalidAvroPayload() throws Exception {
        mvc.perform(get("/js/producer.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.allOf(
                        org.hamcrest.Matchers.containsString("INVALID_AVRO_PAYLOAD"),
                        org.hamcrest.Matchers.containsString("{\"string\": \"x\"}"),
                        org.hamcrest.Matchers.containsString("details"))));
    }

    @Test
    void stylesheetIsServed() throws Exception {
        mvc.perform(get("/css/app.css"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString(".btn-primary")));
    }

    @Test
    void shellScriptsAreServed() throws Exception {
        for (String script : new String[] { "apiClient.js", "app.js", "producer.js" }) {
            mvc.perform(get("/js/" + script))
                    .andExpect(status().isOk());
        }
    }

    @Test
    void apiClientExposesPollerAndProblemDetailParsing() throws Exception {
        mvc.perform(get("/js/apiClient.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.allOf(
                        org.hamcrest.Matchers.containsString("startPolling"),
                        org.hamcrest.Matchers.containsString("visibilitychange"),
                        org.hamcrest.Matchers.containsString("correlationId"))));
    }
}
