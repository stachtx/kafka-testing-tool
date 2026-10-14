plugins {
    java
    id("org.springframework.boot") version "4.1.1"
    id("io.spring.dependency-management") version "1.1.7"
    id("com.google.protobuf") version "0.10.0"
}

group = "com.acme"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
    // Required for io.confluent:* serializers (not published to Maven Central)
    maven {
        url = uri("https://packages.confluent.io/maven/")
        content {
            includeGroup("io.confluent")
            includeGroup("io.confluent.shaded")
        }
    }
}

// Versions not managed by the Spring Boot 4.1.1 BOM (see "Dependencies" in README.md)
val confluentVersion = "8.3.2"
val springdocVersion = "3.1.1"
val archunitVersion = "1.5.1"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-kafka")

    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    implementation("io.confluent:kafka-protobuf-serializer:$confluentVersion")
    implementation("io.confluent:kafka-schema-registry-client:$confluentVersion")
    // protobuf-java / protobuf-java-util versions are managed by the Spring Boot BOM (protobuf-bom)
    implementation("com.google.protobuf:protobuf-java")
    implementation("com.google.protobuf:protobuf-java-util")

    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:$springdocVersion")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("com.tngtech.archunit:archunit-junit5:$archunitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Protobuf: descriptor-set only (ADR-006). No Java codegen; sample .proto files are
// compiled into a FileDescriptorSet that is packaged on the classpath.
val protobufVersion = dependencyManagement.importedProperties["protobuf-java.version"]
    ?: error("protobuf-java.version not found in Spring Boot BOM")

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    generateProtoTasks {
        all().configureEach {
            // descriptor-set only: drop every builtin (java) so no classes are generated
            builtins.clear()
            generateDescriptorSet = true
            descriptorSetOptions.path = layout.buildDirectory
                .file("generated-descriptors/${sourceSet.name}/${sourceSet.name}.desc").get().asFile.path
            descriptorSetOptions.includeImports = true
            descriptorSetOptions.includeSourceInfo = false
        }
    }
}

// Proto sources live under src/main/resources/schemas/proto (ADR-006: no generated classes)
sourceSets {
    main {
        proto {
            setSrcDirs(listOf("src/main/resources/schemas/proto"))
        }
        resources {
            // package descriptor set onto the classpath
            srcDir(layout.buildDirectory.dir("generated-descriptors/main"))
        }
    }
}

tasks.named("processResources") {
    dependsOn("generateProto")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Default `test` skips Docker-based tests tagged "integration" (Group 12). Run them with
// `./gradlew integrationTest` (Docker required; they are skipped, not failed, without Docker).
tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs Testcontainers-based integration tests (tag: integration). Requires Docker."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    shouldRunAfter(tasks.named("test"))
}
