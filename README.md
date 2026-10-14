# Kafka Testing Tool

Internal tool for producing and consuming Avro and Protobuf events against a Kafka cluster and Confluent Schema Registry,
through a small web UI (and a REST API with Swagger UI).
Spring Boot 4.1.1, Java 21, Gradle Kotlin DSL, package `com.acme.kafkatester`.

## TL;DR - run it

| You have | Command | What happens |
|----------|---------|--------------|
| Docker, nothing else | `.\run.ps1` (Windows) or `./run.sh` (Linux/macOS) | starts Kafka + Schema Registry in Docker, starts the app, opens <http://127.0.0.1:8080> |
| An existing cluster | set `KAFKA_BOOTSTRAP_SERVERS` and `SCHEMA_REGISTRY_URL`, then `.\run.ps1 -Remote` / `./run.sh --remote` | starts only the app against your cluster |
| Finished trying it | `.\run.ps1 -Stop` / `./run.sh --stop` | stops the Docker containers and deletes their data |

Stop the app with `Ctrl+C`. The first local start pulls the Docker images and can take a few minutes.

## Requirements

| Needed for | Requirement |
|------------|-------------|
| Always | A JDK to start Gradle (17+; the build itself uses a **Java 21** toolchain, found automatically if installed, e.g. in `~/.jdks` or by your OS package manager). The runner scripts locate a JDK for you (`JAVA_HOME`, `PATH`, `~/.jdks`, Program Files). |
| Local mode (no cluster) | Docker with Docker Compose (Docker Desktop on Windows/macOS) and free ports `9092`, `8081` |
| Remote mode | A reachable Kafka cluster and Schema Registry |
| `integrationTest` | Docker (Testcontainers starts its own throw-away containers) |
| Linux/macOS runner | `bash`, `curl` |

No Node.js or other tooling is needed; the UI is plain static files served by the app.

## Runner scripts

`run.ps1` (PowerShell) and `run.sh` (bash) do the same thing:

1. find a JDK and put it on `PATH` / `JAVA_HOME` for Gradle,
2. **local mode (default)**: `docker compose up -d`, wait until the Schema Registry answers,
   activate the Spring profile `local`;
   **remote mode**: check that the connection variables are set, skip Docker,
3. start the app with `gradlew bootRun`, wait for `/api/kafka/health`, open the browser.

| Windows (`run.ps1`) | Linux/macOS (`run.sh`) | Meaning |
|---------------------|------------------------|---------|
| *(no flags)* | *(no flags)* | local Kafka + Registry in Docker, then the app |
| `-Remote` | `--remote` | use an existing cluster (needs `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`) |
| `-Port 9090` | `--port 9090` | HTTP port (default `8080`) |
| `-Profiles private` | `--profiles private` | extra Spring profiles, comma separated (e.g. `application-private.yml`) |
| `-NoBrowser` | `--no-browser` | do not open the browser |
| `-Stop` | `--stop` | `docker compose down -v` (stops containers, **deletes** local Kafka data) |

Examples:

```powershell
# Windows, local Kafka in Docker
.\run.ps1

# Windows, existing cluster
$env:KAFKA_BOOTSTRAP_SERVERS = 'broker1:9092'
$env:SCHEMA_REGISTRY_URL     = 'http://registry:8081'
.\run.ps1 -Remote
```

```bash
# Linux/macOS (first time: chmod +x run.sh)
./run.sh

KAFKA_BOOTSTRAP_SERVERS=broker1:9092 SCHEMA_REGISTRY_URL=http://registry:8081 ./run.sh --remote
```

If PowerShell refuses to run the script (execution policy), use
`powershell -ExecutionPolicy Bypass -File .\run.ps1`. When the app stops, the Docker containers keep running so the next
start is fast; stop them with `-Stop` / `--stop`.

## Running without the scripts

The scripts only wrap these commands.

```powershell
# local Kafka + Registry
docker compose up -d
.\gradlew.bat bootRun --args="--spring.profiles.active=local"
docker compose down -v                      # when finished

# existing cluster
$env:KAFKA_BOOTSTRAP_SERVERS = 'your-broker:9092'
$env:SCHEMA_REGISTRY_URL     = 'http://your-registry:8081'
.\gradlew.bat bootRun
```

```bash
docker compose up -d
./gradlew bootRun --args='--spring.profiles.active=local'

KAFKA_BOOTSTRAP_SERVERS=your-broker:9092 SCHEMA_REGISTRY_URL=http://your-registry:8081 ./gradlew bootRun
```

`docker-compose.yml` starts a single-broker Kafka (KRaft) on `localhost:9092` and a Schema Registry on
`http://localhost:8081`; the Spring profile **`local`** (`src/main/resources/application-local.yml`) points the app at
exactly that. Topics are auto-created on first use. Under the `local` profile `KAFKA_BOOTSTRAP_SERVERS` and
`SCHEMA_REGISTRY_URL` are ignored (the profile sets fixed values); `SPRING_KAFKA_*` variables still take precedence.

Packaged jar (same environment variables): `./gradlew bootJar`, then `java -jar build/libs/<name>.jar`.

If port 8080 is taken: runner `-Port 9090`, or add `--server.port=9090` to the `--args` value.
The server binds to `127.0.0.1` by default; see "Security model" before setting `SERVER_ADDRESS=0.0.0.0`.

## Using the UI

Open <http://127.0.0.1:8080> (the runner does this for you).

| Screen | URL |
|--------|-----|
| Producer | `/#/producer` |
| Consumers | `/#/consumers` |
| Event Catalog (and schema registration) | `/#/catalog` |
| Swagger UI | `/swagger-ui.html` |
| Health (Kafka and Registry reachability) | `/api/kafka/health` |

### First run with the sample events

1. Open **Event Catalog**: the sample `PingEvent` is listed in both AVRO and PROTOBUF.
2. Click **Register / Update Schema** on the AVRO `PingEvent`, review the diff, confirm. This writes version 1 of
   `sandbox.ping-value` to the registry. (The tool never registers schemas on its own, see "Safety".)
3. Open **Consumers**, topic `sandbox.ping`, format AVRO, offset LATEST, **Start Consumer**. Wait until the card no longer
   says `starting...` (on a brand-new topic this can take several seconds).
4. Open **Producer**, keep topic `sandbox.ping`, format AVRO, event type `PingEvent`, **Load Example**, optionally a key and
   headers, **Send Event**. The event shows up under the consumer; click its row to see key, headers and decoded payload.

Notes:
- **Validate** is a dry run: it encodes the payload against the local schema and sends nothing, and does not contact the
  registry.
- A subject (`<topic>-value`) holds one format: use a different topic for PROTOBUF than for AVRO.
- **EARLIEST** reads from the beginning of the topic and auto-stops after the configured cap (default 1000 records);
  the UI asks for confirmation first.
- Without Kafka the app still starts; the health endpoint reports `DOWN` and produce/consume calls return errors.

## Configuration

Kafka and Schema Registry settings are in `src/main/resources/application.yml` (`spring.kafka.bootstrap-servers`,
`spring.kafka.properties.schema.registry.url`). Each value is `${ENV_VAR:default}`: edit the default in the file, or
override it with the environment variable. The `localhost` values are intentional local-dev defaults; no secrets are
committed. Security settings (SASL/SSL/registry auth) can be added to the same file, e.g.:

```yaml
spring:
  kafka:
    bootstrap-servers: broker1:9093,broker2:9093
    security:
      protocol: SASL_SSL
    properties:
      schema.registry.url: https://registry.example.internal
      sasl.mechanism: PLAIN
      sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p";
      basic.auth.credentials.source: USER_INFO
      basic.auth.user.info: user:password
    ssl:
      trust-store-location: file:/path/truststore.jks
      trust-store-password: changeit
```

Do not commit real endpoints or secrets: keep them in a git-ignored `application-private.yml` (next to
`application.yml`, or in `./config/`, also git-ignored) and activate it with the profile `private`
(`.\run.ps1 -Remote -Profiles private`, or `--spring.profiles.active=private`), or use environment variables.
Profiles can be combined, e.g. `--spring.profiles.active=local,private`.

### Environment variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `SCHEMA_REGISTRY_URL` | Schema Registry URL | `http://localhost:8081` |
| `KAFKATESTER_CONSUMER_EARLIEST_CAP` | Max records for an EARLIEST consumer session (min 1) | `1000` |
| `KAFKATESTER_CONSUMER_MAX_SESSIONS` | Max concurrent consumer sessions; more starts get `429 CONSUMER_LIMIT_REACHED` (min 1) | `10` |
| `KAFKATESTER_CONSUMER_IDLE_TTL` | Sessions not polled for this long are torn down by a sweeper (Spring duration, e.g. `30m`) | `30m` |
| `KAFKATESTER_CONSUMER_MAX_PAYLOAD_BYTES` | Decoded event payloads above this size are truncated and flagged `payloadTruncated` (min 1) | `65536` |
| `SERVER_ADDRESS` | Interface the HTTP server binds to | `127.0.0.1` |
| `SPRINGDOC_ENABLED` | Serves Swagger UI (`/swagger-ui.html`) and OpenAPI JSON (`/v3/api-docs`) | `true` |
| `SPRING_KAFKA_SECURITY_PROTOCOL` | e.g. `SASL_SSL` | `PLAINTEXT` |
| `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` | e.g. `PLAIN` | unset |
| `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG` | JAAS config (secret) | unset |
| `SPRING_KAFKA_PROPERTIES_BASIC_AUTH_CREDENTIALS_SOURCE` | Registry auth source (`USER_INFO`) | unset |
| `SPRING_KAFKA_PROPERTIES_BASIC_AUTH_USER_INFO` | Registry `user:password` (secret) | unset |
| `SPRING_KAFKA_SSL_TRUST_STORE_LOCATION` / `_PASSWORD` | Truststore | unset |
| `SPRING_KAFKA_SSL_KEY_STORE_LOCATION` / `_PASSWORD` | Keystore | unset |

Any other `spring.kafka.*` or `spring.kafka.properties.*` setting can be supplied the same way through Spring's relaxed
binding of `SPRING_KAFKA_*` variables. Secrets are never logged.

Optional property: `kafkatester.consumer.sweep-interval` (default `PT1M`) sets how often idle consumer sessions are swept.

## IntelliJ IDEA

1. **Open** the project folder (`File > Open`, select the folder or `build.gradle.kts`) and let Gradle import finish.
2. **JDK 21**: `File > Project Structure > Project > SDK` = a JDK 21, and
   `Settings > Build, Execution, Deployment > Build Tools > Gradle > Gradle JVM` = the same JDK 21.
3. **Build with Gradle** (important): in the same Gradle settings page set *Build and run using* = `Gradle`.
   The Protobuf descriptor set (`main.desc`) is generated by the Gradle `generateProto` task; if IntelliJ builds on its own
   the Protobuf events will be missing from the Event Catalog.
4. **Run**, either way:
   - Gradle tool window: `Tasks > application > bootRun` (right click > *Run* or *Debug*), or
   - run `KafkaTesterApplication` (`com.acme.kafkatester`) via the green gutter icon.
5. **Environment / profile**: `Run > Edit Configurations`, select the configuration, *Modify options > Environment
   variables*, e.g. `KAFKA_BOOTSTRAP_SERVERS=your-broker:9092;SCHEMA_REGISTRY_URL=http://your-registry:8081`, or put `local` in
   *Active profiles* to use the Docker setup (start it first with `docker compose up -d`). Optional VM option:
   `-Dserver.port=9090`.
6. Open <http://127.0.0.1:8080>. The console log shows `Started KafkaTesterApplication`.

## Build and test

```
./gradlew build            # compile, unit tests, bootJar (build/libs/*.jar)
./gradlew test             # default suite, Docker not required
./gradlew integrationTest  # Testcontainers tests, requires a running Docker daemon
```

(Windows: `.\gradlew.bat ...`.)

- `test` runs the unit, architecture (ArchUnit) and config tests. It excludes tests tagged `integration`.
- `integrationTest` runs the Testcontainers tests (Kafka `confluentinc/cp-kafka:8.3.2` and Schema Registry
  `confluentinc/cp-schema-registry:8.3.2` in throw-away containers; no real cluster is contacted).
  **Docker must be running.** Without a Docker daemon the tests are reported as skipped, not failed, so a skipped run
  verifies nothing against a real broker. The first run pulls the images and takes a few minutes.
- The integration tests cover: Avro and Protobuf `PingEvent` producer round trips (register, then send), consumer
  lifecycle (LATEST `seekToEnd`, EARLIEST cap auto-stop, stop teardown, lossless cursor polling) and registration
  (preview is read-only, `POST /api/schemas/register` writes exactly once, a changed schema becomes version 2).

## Security model

The tool has **no authentication or authorization**. It is an internal, trusted DEV tool: anyone who can reach its HTTP
port can produce to and consume from Kafka, and register schemas (`POST /api/schemas/register`), using the tool's own
Kafka and Registry credentials. Therefore it binds to loopback (`127.0.0.1`) by default. Set `SERVER_ADDRESS=0.0.0.0`
(or a specific interface) only as a conscious decision, on a network you trust, ideally behind your own access
control. Swagger UI and the OpenAPI document are enabled by default (harmless on loopback); set `SPRINGDOC_ENABLED=false`
to turn them off when exposing the tool more widely. Consumer sessions are bounded (`KAFKATESTER_CONSUMER_MAX_SESSIONS`,
idle TTL, payload size cap) so a forgotten browser tab cannot exhaust the tool. The web UI ships a strict
Content-Security-Policy (`script-src 'self'`, no inline code).

## Safety

The tool never registers schemas implicitly (`auto.register.schemas=false`). Registration is a separate, explicit action.

Serializers are pinned to `use.latest.version=true`, `latest.compatibility.strict=true` and `TopicNameStrategy`
(`<topic>-value`); deserializers decode by the writer schema id (`use.latest.version=false`). Before every send the
producer compares the local catalog schema with the registry's latest version of `<topic>-value`; if the subject is
missing or the schemas differ the send is refused (HTTP 409, `SCHEMA_REGISTRY_ERROR`) until the new version has been
registered via the Event Catalog. Validate (dry run) does not contact the registry, so a "valid" result does not
guarantee that the registry check on send will pass (send can still answer 409). Known limitation: if the subject was
registered by someone else with different docs/whitespace, or the registry uses `normalize=true`, the guard keeps
answering 409 until a new version is registered via the Event Catalog. The "latest schema" caches expire after 5 s
(`latest.cache.ttl.sec`), so a version registered in the UI is used by the next send within seconds.

## Schemas and the catalog

Add `*.avsc` files under `src/main/resources/schemas/avro/` and `*.proto` files under
`src/main/resources/schemas/proto/`; they are discovered at startup, no Java change needed. Event types are keyed by
simple name per format: two records/messages with the same simple name in one format (even in different namespaces)
fail startup with a clear error, as does a Protobuf descriptor with missing dependencies.

The suggested ("default") topic of an event type is declared in the sidecar
`src/main/resources/schemas/default-topics.properties` (`EventName=topic`, applies to Avro and Protobuf). It is kept
out of the schemas on purpose so tool-private metadata is never registered into the shared registry. (A legacy Avro
top-level `"defaultTopic"` property is still read as a fallback and is stripped before preview/compare/registration;
there is no Protobuf option any more.) Protobuf imports other than `google/*` well-known types are not supported for
registration.

## Dependencies

Gradle resolves everything on first build; nothing to install by hand. Key versions (see `build.gradle.kts`):

| Item | Version |
|------|---------|
| Java toolchain | 21 |
| Gradle wrapper | 9.8.1 |
| Spring Boot | 4.1.1 (manages Kafka clients 4.2.1, Spring Kafka 4.1.1, Jackson 3, protobuf-java 4.35.1, Testcontainers 2.0.5) |
| Confluent serializers / registry client | 8.3.2 (from `https://packages.confluent.io/maven/`, not on Maven Central) |
| springdoc-openapi | 3.1.1 |
| ArchUnit | 1.5.1 |
| Protobuf Gradle plugin | 0.10.0 (descriptor set only, no generated Java) |

Spring Boot 4 uses Jackson 3 for the web layer; the Confluent libraries still use Jackson 2 internally. Both coexist on
the classpath (different packages).
