package com.acme.kafkatester.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.constructors;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.acme.kafkatester.registry.SchemaRegistrationService;
import com.acme.kafkatester.web.rest.SchemaRegistrationController;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaCall;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Safety net for ADR-001 (consumer must not depend on the catalog) and ADR-003 (schema registration
 * is isolated: one write class, one injector).
 *
 * <p>The rules are built by static factories so that the same rule objects can be evaluated against
 * deliberately violating fixture classes ({@code architecture/fixture/**}, test sources only, never
 * part of the production import). The negative test proves each rule really fails when a class
 * breaks it, i.e. the rules are not vacuous.
 */
class RegistrationIsolationArchTest {

    private static final String BASE = "com.acme.kafkatester";
    private static final String SERVICE = SchemaRegistrationService.class.getName();
    private static final String CONTROLLER = SchemaRegistrationController.class.getName();
    private static final String FIXTURES = BASE + ".architecture.fixture";

    /** Registry client methods that mutate registry state; "register" is the contractual one. */
    private static final Set<String> WRITE_PREFIXES = Set.of("register", "delete", "update", "setMode");

    private static com.tngtech.archunit.core.domain.JavaClasses production;
    private static com.tngtech.archunit.core.domain.JavaClasses fixtures;

    @BeforeAll
    static void importClasses() {
        production = new ClassFileImporter().withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages(BASE);
        fixtures = new ClassFileImporter().importPackages(FIXTURES);
    }

    // ---- rule factories ----------------------------------------------------------------------

    /** No class other than SchemaRegistrationService may call a mutating SchemaRegistryClient method. */
    static ArchRule onlyServiceUsesRegisterApi() {
        ArchCondition<JavaClass> notCallWriteApi = new ArchCondition<>("not call the Schema Registry client's register API") {
            @Override
            public void check(JavaClass item, ConditionEvents events) {
                for (JavaAccess<?> access : item.getAccessesFromSelf()) {
                    // JavaCall covers method calls and method references (client::register)
                    if (access instanceof JavaCall<?> call && isRegistryWrite(call)) {
                        events.add(SimpleConditionEvent.violated(call, call.getDescription()));
                    }
                }
            }
        };
        return classes().that(new DescribedPredicate<JavaClass>("are not SchemaRegistrationService") {
            @Override
            public boolean test(JavaClass c) {
                return !c.getName().equals(SERVICE);
            }
        }).should(notCallWriteApi);
    }

    private static boolean isRegistryWrite(JavaCall<?> call) {
        String name = call.getTarget().getName();
        return WRITE_PREFIXES.stream().anyMatch(name::startsWith)
                && call.getTargetOwner().isAssignableTo(SchemaRegistryClient.class);
    }

    /** Only the controller (and the registry package itself) may depend on SchemaRegistrationService. */
    static ArchRule serviceInjectedOnlyByController() {
        return noClasses().that().doNotHaveFullyQualifiedName(CONTROLLER)
                .and().resideOutsideOfPackage(BASE + ".registry..")
                .should().dependOnClassesThat().haveFullyQualifiedName(SERVICE)
                .because("ADR-003: the single registry write path is reachable from one controller only");
    }

    static ArchRule serviceConstructorPackagePrivate() {
        return constructors().that().areDeclaredIn(SERVICE).should().bePackagePrivate()
                .because("ADR-003: no code outside the registry package can instantiate the write path");
    }

    static ArchRule packageIndependentOf(String fromPackage, String toPackage) {
        return noClasses().that().resideInAPackage(fromPackage)
                .should().dependOnClassesThat().resideInAPackage(toPackage);
    }

    static ArchRule packageDoesNotUseService(String fromPackage) {
        return noClasses().that().resideInAPackage(fromPackage)
                .should().dependOnClassesThat().haveFullyQualifiedName(SERVICE);
    }

    // ---- production checks -------------------------------------------------------------------

    @Test
    void onlySchemaRegistrationServiceCallsTheRegistryRegisterApi() {
        // sanity: production scan is real and does not include the violating fixtures
        assertThat(production.contain(SchemaRegistrationService.class)).isTrue();
        assertThat(production.stream().map(JavaClass::getName)).noneMatch(n -> n.startsWith(FIXTURES));

        onlyServiceUsesRegisterApi().check(production);
    }

    @Test
    void schemaRegistrationServiceIsInjectedOnlyIntoTheRegistrationController() {
        serviceInjectedOnlyByController().check(production);
        serviceConstructorPackagePrivate().check(production);
    }

    @Test
    void consumerPackageDoesNotDependOnCatalog() {
        packageIndependentOf(BASE + ".consumer..", BASE + ".catalog..").check(production);
    }

    @Test
    void producerAndConsumerDoNotReferenceSchemaRegistrationService() {
        packageDoesNotUseService(BASE + ".producer..").check(production);
        packageDoesNotUseService(BASE + ".consumer..").check(production);
    }

    // ---- negative proof: the rules fail on violating fixtures --------------------------------

    @Test
    void rulesFailWhenAFixtureClassViolatesThem() {
        assertThat(fixtures.contain(FIXTURES + ".RogueRegistrar")).isTrue();

        assertThatThrownBy(() -> onlyServiceUsesRegisterApi().check(fixtures))
                .isInstanceOf(AssertionError.class).hasMessageContaining("RogueRegistrar");
        assertThatThrownBy(() -> serviceInjectedOnlyByController().check(fixtures))
                .isInstanceOf(AssertionError.class).hasMessageContaining("RogueServiceInjector");
        assertThatThrownBy(() -> packageIndependentOf(FIXTURES + ".consumer..", BASE + ".catalog..").check(fixtures))
                .isInstanceOf(AssertionError.class).hasMessageContaining("CatalogPeekingConsumer");
        assertThatThrownBy(() -> packageDoesNotUseService(FIXTURES + ".producer..").check(fixtures))
                .isInstanceOf(AssertionError.class).hasMessageContaining("RegistrationUsingProducer");
    }
}
