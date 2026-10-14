#!/usr/bin/env bash
# One-command runner for Kafka Testing Tool (Linux / macOS).
#
#   ./run.sh                       local Kafka + Schema Registry in Docker, then the app, then opens the browser
#   ./run.sh --port 9090           different HTTP port
#   ./run.sh --remote              existing cluster; needs KAFKA_BOOTSTRAP_SERVERS and SCHEMA_REGISTRY_URL
#   ./run.sh --remote --profiles private   plus application-private.yml
#   ./run.sh --stop                stop the local Kafka/Registry containers and delete their data
#   ./run.sh --no-browser          do not open the browser
set -euo pipefail
cd "$(dirname "$0")"

REMOTE=0; STOP=0; BROWSER=1; PORT=8080; EXTRA_PROFILES=""

while [ $# -gt 0 ]; do
  case "$1" in
    --remote) REMOTE=1 ;;
    --stop) STOP=1 ;;
    --no-browser) BROWSER=0 ;;
    --port) PORT="${2:?--port needs a value}"; shift ;;
    --profiles) EXTRA_PROFILES="${2:?--profiles needs a value}"; shift ;;
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1 (see --help)" >&2; exit 1 ;;
  esac
  shift
done

step() { printf '\033[36m==> %s\033[0m\n' "$1"; }
fail() { printf '\033[31mERROR: %s\033[0m\n' "$1" >&2; exit 1; }

check_docker() {
  command -v docker >/dev/null 2>&1 || fail "Docker not found. Install Docker, or use --remote with an existing cluster."
  docker info >/dev/null 2>&1 || fail "Docker daemon is not running. Start Docker and retry."
}

if [ "$STOP" = 1 ]; then
  check_docker
  step "Stopping local Kafka + Schema Registry (data is deleted)"
  docker compose down -v
  exit 0
fi

# Gradle only needs a JDK 17+ to start; it then finds the Java 21 toolchain by itself.
if [ -z "${JAVA_HOME:-}" ] && ! command -v java >/dev/null 2>&1; then
  fail "No JDK found. Install JDK 21 (e.g. Temurin/Corretto) or set JAVA_HOME."
fi

PROFILES=""
if [ "$REMOTE" = 1 ]; then
  [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ] && [ -n "${SCHEMA_REGISTRY_URL:-}" ] \
    || fail "Remote mode needs KAFKA_BOOTSTRAP_SERVERS and SCHEMA_REGISTRY_URL (e.g. export KAFKA_BOOTSTRAP_SERVERS=broker:9092)."
  step "Remote mode: Kafka ${KAFKA_BOOTSTRAP_SERVERS}, Registry ${SCHEMA_REGISTRY_URL}"
else
  check_docker
  step "Starting local Kafka + Schema Registry (docker compose up -d)"
  docker compose up -d
  step "Waiting for Schema Registry on http://localhost:8081 (first start pulls images, can take minutes)"
  ready=0
  for _ in $(seq 1 90); do
    if curl -fsS -m 3 http://localhost:8081/subjects >/dev/null 2>&1; then ready=1; break; fi
    sleep 2
  done
  [ "$ready" = 1 ] || fail "Schema Registry did not become ready in 3 minutes. Check: docker compose logs"
  PROFILES="local"
fi
if [ -n "$EXTRA_PROFILES" ]; then PROFILES="${PROFILES:+$PROFILES,}$EXTRA_PROFILES"; fi

APP_ARGS="--server.port=$PORT"
[ -n "$PROFILES" ] && APP_ARGS="$APP_ARGS --spring.profiles.active=$PROFILES"
URL="http://127.0.0.1:$PORT"

if [ "$BROWSER" = 1 ]; then
  (
    for _ in $(seq 1 180); do
      if curl -fsS -m 2 "$URL/api/kafka/health" >/dev/null 2>&1; then
        (xdg-open "$URL" || open "$URL") >/dev/null 2>&1 || true
        break
      fi
      sleep 2
    done
  ) &
fi

cleanup() {
  jobs -p | xargs -r kill 2>/dev/null || true
  [ "$REMOTE" = 1 ] || printf '\n\033[33mApp stopped. Kafka + Registry containers are still running; stop them with: ./run.sh --stop\033[0m\n'
}
trap cleanup EXIT

step "Starting app on $URL (Ctrl+C to stop)"
./gradlew bootRun "--args=$APP_ARGS" --console=plain
