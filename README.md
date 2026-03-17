# gap — GAP → HyperDX / ClickHouse Migration Tool

A single self-contained binary that migrates observability configuration from a **Grafana / Alertmanager / Prometheus (GAP)** stack to **HyperDX** (ClickStack), including historical Prometheus metric backfill into **ClickHouse**.

---

## Project Layout

```
gap_tool/
├── cmd/
│   └── gap/
│       └── main.go           ← single unified binary entrypoint
├── internal/
│   └── gaptool/
│       ├── fetch_validate.go       ← fetch GAP configs, validate migration
│       ├── grafana_to_hyperdx.go   ← convert Grafana dashboards → HyperDX format
│       ├── promql_to_hyperdx.go    ← convert PromQL / alert rules → HyperDX format
│       ├── migration.go            ← orchestrate convert + apply (dashboards & alerts)
│       ├── prometheus_backfill.go  ← backfill historical Prometheus data → ClickHouse
│       ├── validation.go           ← compare source vs target state
│       ├── hyperdx_client.go       ← HyperDX REST API client
│       ├── split_cli.go            ← public Run*CLI() entry points (stdin → stdout JSON)
│       ├── split_requests.go       ← request struct definitions
│       └── helpers.go              ← shared type-conversion utilities
├── go.mod
└── README.md
```

---

## Build

```bash
# Build the binary (outputs gap.exe on Windows, gap on Linux/macOS)
go build -o gap ./cmd/gap

# Run all tests
go test ./...
```

> **Requires Go 1.22+**

---

## Usage

All subcommands read a **JSON request** from `stdin` and write a **JSON response** to `stdout`. Diagnostic logs go to `stderr`.

```
gap <subcommand>
```

| Subcommand | What it does |
|---|---|
| `fetch` | Fetch all configs from Grafana, Prometheus, and Alertmanager |
| `validate` | Compare source (GAP) state with current HyperDX state |
| `convert-grafana` | Convert Grafana dashboards → HyperDX format (no writes) |
| `apply-grafana` | Convert and POST Grafana dashboards to HyperDX |
| `convert-alerts` | Convert Prometheus alert rules → HyperDX format (no writes) |
| `apply-alerts` | Convert and POST alert rules + dashboards to HyperDX |
| `backfill` | Backfill historical Prometheus metrics into ClickHouse |

---

## Step-by-Step Migration Guide

### Step 1 — Fetch your current GAP configuration

Pulls dashboards from Grafana, alert rules from Prometheus, and routing config from Alertmanager. Grafana dashboards are fetched in parallel (up to 10 concurrent).

```bash
echo '{
  "grafana_url":      "http://grafana:3000",
  "grafana_api_key":  "glsa_YOUR_SERVICE_ACCOUNT_TOKEN",
  "prometheus_url":   "http://prometheus:9090",
  "alertmanager_url": "http://alertmanager:9093"
}' | ./gap fetch > gap_data.json
```

**Output fields:** `status`, `data.grafana`, `data.prometheus`, `data.alertmanager`

---

### Step 2 — Convert Grafana dashboards (dry run / inspect)

Converts the fetched Grafana dashboards to HyperDX dashboard payloads. No network writes — good for inspection.

```bash
echo "{
  \"gap_data\": $(cat gap_data.json | jq '.data'),
  \"hyperdx_metric_source_id\": \"YOUR_METRIC_SOURCE_ID\"
}" | ./gap convert-grafana > converted_dashboards.json
```

**Output fields:** `status`, `dashboard_count`, `dashboards` (array of HyperDX payloads)

---

### Step 3 — Apply Grafana dashboards to HyperDX

POSTs the converted dashboards to HyperDX. Add `"dry_run": true` to see what would be created without writing anything.

```bash
echo "{
  \"hyperdx_url\":    \"http://hyperdx:8080\",
  \"hyperdx_api_key\": \"YOUR_HYPERDX_API_KEY\",
  \"dashboards\": $(cat converted_dashboards.json | jq '.dashboards'),
  \"dry_run\": false
}" | ./gap apply-grafana
```

**Output fields:** `status`, `message`, `dashboard_count`

---

### Step 4 — Convert Prometheus alert rules (dry run / inspect)

Converts all Prometheus alerting rules to HyperDX alert + dashboard pairs. Each alert gets its own dedicated dashboard tile.

```bash
echo "{
  \"gap_data\": $(cat gap_data.json | jq '.data'),
  \"hyperdx_metric_source_id\": \"YOUR_METRIC_SOURCE_ID\",
  \"webhook_id\": \"YOUR_HYPERDX_WEBHOOK_ID\"
}" | ./gap convert-alerts > converted_alerts.json
```

**Output fields:** `status`, `alert_count`, `alert_pairs`

---

### Step 5 — Apply alert rules to HyperDX

Creates the alert dashboards and alert rules in HyperDX. Each alert pair is a `{ dashboard, alert }` object — the dashboard is created first, then the alert is linked to the first tile.

```bash
echo "{
  \"hyperdx_url\":    \"http://hyperdx:8080\",
  \"hyperdx_api_key\": \"YOUR_HYPERDX_API_KEY\",
  \"alert_pairs\": $(cat converted_alerts.json | jq '.alert_pairs'),
  \"dry_run\": false
}" | ./gap apply-alerts
```

**Output fields:** `status`, `message`, `alert_count`

---

### Step 6 — Backfill historical Prometheus metrics into ClickHouse

Reads historical time-series data from Prometheus and bulk-inserts it directly into ClickHouse OTel metric tables (`otel_metrics_gauge`, `otel_metrics_sum`, `otel_metrics_histogram`, `otel_metrics_summary`).

All fields have sensible defaults — pass `{}` to use all defaults.

```bash
echo '{
  "prometheus_url":       "http://prometheus:9090",
  "clickhouse_host":      "clickhouse",
  "clickhouse_port":      8123,
  "clickhouse_database":  "default",
  "clickhouse_username":  "default",
  "clickhouse_password":  "",
  "lookback_days":        3,
  "step_seconds":         60,
  "batch_size":           10000,
  "metric_filter":        "",
  "service_name":         "prometheus-migrated",
  "dry_run":              false
}' | ./gap backfill
```

**Key request fields:**

| Field | Default | Description |
|---|---|---|
| `prometheus_url` | `http://localhost:9090` | Prometheus base URL |
| `clickhouse_host` | `localhost` | ClickHouse hostname |
| `clickhouse_port` | `8123` | ClickHouse HTTP port |
| `clickhouse_database` | `default` | Target ClickHouse database |
| `lookback_days` | `3` | How many days of history to backfill |
| `step_seconds` | `60` | Query resolution (seconds) |
| `batch_size` | `10000` | Rows per ClickHouse INSERT batch |
| `metric_filter` | *(skip scrape_ / ALERTS / up)* | Optional regex to filter metrics |
| `service_name` | `prometheus-migrated` | `ServiceName` tag in OTel rows |
| `dry_run` | `false` | If `true`, query but don't insert |

**Output fields:** `status`, `message`, `stats.metrics_discovered`, `stats.metrics_migrated`, `stats.metrics_failed`, `stats.rows_inserted`, `stats.errors`

---

### Step 7 — Validate the migration

Fetches a fresh snapshot from GAP + HyperDX and returns a side-by-side comparison. Useful to confirm what migrated successfully.

```bash
echo '{
  "grafana_url":      "http://grafana:3000",
  "grafana_api_key":  "glsa_YOUR_SERVICE_ACCOUNT_TOKEN",
  "prometheus_url":   "http://prometheus:9090",
  "alertmanager_url": "http://alertmanager:9093",
  "hyperdx_url":      "http://hyperdx:8080",
  "hyperdx_api_key":  "YOUR_HYPERDX_API_KEY"
}' | ./gap validate
```

**Output fields:** `status`, `source_dashboards`, `target_dashboards`, `source_alerts`, `target_alerts`

---

## Environment Variables

| Variable | Description |
|---|---|
| `GAPTOOL_INSECURE_SKIP_VERIFY=true` | Skip TLS certificate verification for all HTTP clients |

---

## Error Handling

All subcommands output a JSON object on both success and failure:

```json
{ "status": true,  "message": "..." }   // success (exit 0)
{ "status": false, "message": "..." }   // failure (exit 1)
```

The `backfill` command additionally returns partial results on failure — check `stats.errors` for per-metric error details.
