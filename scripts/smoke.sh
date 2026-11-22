#!/usr/bin/env bash
# End-to-end check of the whole stack with real Redpanda, Prometheus and
# Grafana: replay a synthetic sample, then confirm the count arrives at every
# hop. Runs in CI; works locally too (needs docker, curl, python3 with numpy).
#
#   bash scripts/smoke.sh            # leaves the stack running afterwards
#   KEEP=0 bash scripts/smoke.sh     # tears it down at the end
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS=${ROWS:-20000}
METRICS_PORT=${METRICS_PORT:-9898}
PROMETHEUS_PORT=${PROMETHEUS_PORT:-9090}
GRAFANA_PORT=${GRAFANA_PORT:-3000}
SAMPLE_DIR=$(mktemp -d)
GROUP_HINT="consumer group detektive-consumer"

log() { printf '\n== %s\n' "$*"; }

# wait_for <description> <seconds> <command...>: retry until the command succeeds
wait_for() {
    local what=$1 limit=$2
    shift 2
    for _ in $(seq "$limit"); do
        if "$@" >/dev/null 2>&1; then
            echo "ok: $what"
            return 0
        fi
        sleep 1
    done
    echo "FAILED: $what (after ${limit}s)"
    docker compose ps
    docker compose logs --tail 50
    return 1
}

processed_on_consumer() {
    curl -fsS localhost:$METRICS_PORT/metrics | grep -qx "detektive_consumer_messages_total{outcome=\"processed\"} $ROWS"
}

# exactly one consumer target up: two would mean the same consumer is
# scraped twice and every sum on the dashboard is doubled
one_consumer_target_up() {
    curl -fsS -G localhost:$PROMETHEUS_PORT/api/v1/query \
        --data-urlencode 'query=sum(up{job="detektive-consumer"})' |
        python3 -c "import json,sys; r=json.load(sys.stdin)['data']['result']; sys.exit(0 if r and float(r[0]['value'][1]) == 1 else 1)"
}

processed_in_prometheus() {
    curl -fsS -G localhost:$PROMETHEUS_PORT/api/v1/query \
        --data-urlencode 'query=sum(detektive_consumer_messages_total{outcome="processed"})' |
        python3 -c "import json,sys; r=json.load(sys.stdin)['data']['result']; sys.exit(0 if r and float(r[0]['value'][1]) == $ROWS else 1)"
}

grafana_has_dashboard() {
    curl -fsS localhost:$GRAFANA_PORT/api/dashboards/uid/detektive-stream | grep -q '"title":"Detektive: payment stream"'
}

grafana_reaches_prometheus() {
    curl -fsS localhost:$GRAFANA_PORT/api/datasources/uid/prometheus/health | grep -q '"status":"OK"'
}

log "checking configuration"
docker compose config -q
docker run --rm -v "$PWD/observability/prometheus.yml:/p.yml:ro" --entrypoint promtool \
    prom/prometheus:v3.16.0 check config /p.yml

log "building the image"
docker compose build consumer

log "starting the stack (fresh volumes)"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --wait redpanda
docker compose up -d consumer prometheus grafana

log "replaying $ROWS synthetic transactions"
PYTHONPATH=ml/src python3 -m detektive_ml.synthetic "$SAMPLE_DIR/HI-Small_Trans.csv" --rows "$ROWS"
chmod -R a+rX "$SAMPLE_DIR"
DETEKTIVE_DATA_DIR=$SAMPLE_DIR docker compose run --rm replay --speed max

log "following the count through every hop"
wait_for "consumer handled all $ROWS ($GROUP_HINT)" 90 processed_on_consumer
wait_for "Prometheus scrapes exactly one consumer" 30 one_consumer_target_up
wait_for "Prometheus scraped the same count" 60 processed_in_prometheus
wait_for "Grafana loaded the dashboard" 60 grafana_has_dashboard
wait_for "Grafana can query Prometheus" 30 grafana_reaches_prometheus

log "all checks passed"
rm -rf "$SAMPLE_DIR"
if [ "${KEEP:-1}" = "0" ]; then
    docker compose down -v
fi
