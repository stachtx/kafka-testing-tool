package com.acme.kafkatester.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.acme.kafkatester.registry.SchemaRegistryGateway;
import com.acme.kafkatester.security.KafkaSecurityConfig;
import com.acme.kafkatester.web.rest.HealthController;

/**
 * Verifies the Schema Registration modal script (Group 11) is served, is wired to the preview and register
 * endpoints, and that it is the ONLY UI script able to trigger registration (ADR-003).
 *
 * <h2>Manual verification checklist (Schema Registration modal)</h2>
 * Start the app (`./gradlew bootRun`), open http://localhost:8080/#/catalog and click "Register / Update Schema".
 * <ol>
 *   <li><b>Preview before confirm:</b> the modal "Register Schema to Shared DEV Registry" opens with the warning copy,
 *       the "&lt;EventType&gt; - compatibility: MODE" line + pill, and "Loading preview..." while
 *       {@code GET /api/schemas/{eventType}/registration-preview} is in flight. The red "Register New Version to DEV
 *       Registry" button is DISABLED until the preview loaded and reports {@code compatible=true} and
 *       {@code identical=false}. Two panes show "Registry (latest, vN)" and "Local schema (to register)".</li>
 *   <li><b>Cancel makes no write call:</b> Cancel, Esc and a backdrop click all close the modal; the network log shows
 *       only the GET preview and no POST.</li>
 *   <li><b>Confirm issues exactly one POST:</b> click confirm (even a rapid double click): one
 *       {@code POST /api/schemas/register {eventType, format}} is sent; the modal then shows subject, version and
 *       schema id. A registry failure ({@code SCHEMA_REGISTRY_ERROR}) is shown inside the modal.</li>
 *   <li><b>Incompatible blocks confirm:</b> when the preview has {@code compatible=false} the compatibility messages
 *       are listed and confirm stays disabled with an explanatory message; same for {@code identical=true} (no changes)
 *       and for a failed preview (ProblemDetail shown, confirm disabled).</li>
 *   <li><b>First registration:</b> the registry pane says no schema is registered yet.</li>
 * </ol>
 */
@WebMvcTest(controllers = HealthController.class)
class RegistrationUiTest {

    private static final Path JS_DIR = Path.of("src", "main", "resources", "static", "js");
    private static final String REGISTER_PATH = "/api/schemas/register";

    @Autowired MockMvc mvc;

    @MockitoBean KafkaSecurityConfig securityConfig;
    @MockitoBean SchemaRegistryGateway gateway;

    private String servedRegistrationScript() throws Exception {
        MvcResult result = mvc.perform(get("/js/registration.js"))
                .andExpect(status().isOk())
                .andReturn();
        return result.getResponse().getContentAsString(StandardCharsets.UTF_8);
    }

    @Test
    void registrationScriptIsServed() throws Exception {
        mvc.perform(get("/js/registration.js")).andExpect(status().isOk());
    }

    @Test
    void listensForRegisterSchemaEventAndUsesModalHelper() throws Exception {
        mvc.perform(get("/js/registration.js"))
                .andExpect(status().isOk())
                .andExpect(content().string(Matchers.allOf(
                        Matchers.containsString("addEventListener('register-schema'"),
                        Matchers.containsString("App.openModal"),
                        Matchers.containsString("danger: true"),
                        Matchers.containsString("Register Schema to Shared DEV Registry"),
                        Matchers.containsString("Register New Version to DEV Registry"))));
    }

    @Test
    void callsPreviewAndGatesConfirmOnIt() throws Exception {
        String js = servedRegistrationScript();
        assertTrue(js.contains("registration-preview"), "must fetch the registration preview");
        assertTrue(js.contains("compatible"), "must inspect compatibility");
        assertTrue(js.contains("identical"), "must inspect identical flag");
        int previewCall = js.indexOf("registration-preview");
        int firstEnable = js.indexOf("confirmBtn.disabled = false");
        assertTrue(firstEnable > previewCall, "confirm may only be enabled after the preview call is defined/handled");
        assertEquals(1, count(js, "confirmBtn.disabled = false"), "confirm is enabled in exactly one place");
    }

    @Test
    void containsExactlyOneRegisterPost() throws Exception {
        String js = servedRegistrationScript();
        assertEquals(1, count(js, REGISTER_PATH), "exactly one reference to the register endpoint");
        assertEquals(1, count(js, "ApiClient.post("), "exactly one POST call");
    }

    @Test
    void registrationScriptIsTheOnlyScriptReferencingRegisterEndpoint() throws IOException {
        assertTrue(Files.isDirectory(JS_DIR), "static js dir not found from " + Path.of("").toAbsolutePath());
        try (Stream<Path> files = Files.walk(JS_DIR)) {
            List<Path> offenders = files
                    .filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().equals("registration.js"))
                    .filter(p -> {
                        try {
                            return Files.readString(p, StandardCharsets.UTF_8).contains(REGISTER_PATH);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    })
                    .toList();
            assertTrue(offenders.isEmpty(), "only registration.js may reference " + REGISTER_PATH + ": " + offenders);
        }
        try (Stream<Path> all = Files.walk(Path.of("src", "main", "resources", "static"))) {
            assertFalse(all.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".html"))
                    .anyMatch(p -> {
                        try {
                            return Files.readString(p, StandardCharsets.UTF_8).contains(REGISTER_PATH);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }), "html files must not reference the register endpoint");
        }
    }

    private static int count(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }
}
