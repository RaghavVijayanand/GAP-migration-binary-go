# gap — GAP → HyperDX / ClickHouse Migration Tool

Migrates observability configuration from a **Grafana / Alertmanager / Prometheus (GAP)** stack to **HyperDX** (ClickStack), with optional historical metric backfill into **ClickHouse**.

---

## Quick Start

```bash
# 1. Build the binary
go build -o gap ./cmd/gap

# 2. Copy and fill in your config
cp .env.example .env
$EDITOR .env

# 3. Run the migration
python migrate.py
```

Add `--dry-run` to preview what will be created without writing anything.

---

## Configuration (`.env`)

```bash
# Required
GAP_GRAFANA_URL=http://grafana:3000
GAP_GRAFANA_API_KEY=glsa_YOUR_SERVICE_ACCOUNT_TOKEN
GAP_PROMETHEUS_URL=http://prometheus:9090
GAP_HYPERDX_URL=http://hyperdx:8080
GAP_HYPERDX_API_KEY=YOUR_HYPERDX_API_KEY

# Optional — auto-discovered from the HyperDX API if left blank
GAP_METRIC_SOURCE_ID=     # first source is used if blank
GAP_WEBHOOK_ID=           # first webhook is used if blank; empty = no notifications

# Optional source
GAP_ALERTMANAGER_URL=     # skip if not using Alertmanager

# Backfill (only needed with --backfill)
GAP_CLICKHOUSE_HOST=localhost
GAP_CLICKHOUSE_PORT=8123
GAP_CLICKHOUSE_DATABASE=default
GAP_CLICKHOUSE_USERNAME=default
GAP_CLICKHOUSE_PASSWORD=
GAP_LOOKBACK_DAYS=3
GAP_STEP_SECONDS=60
GAP_BATCH_SIZE=10000
GAP_METRIC_FILTER=
GAP_SERVICE_NAME=prometheus-migrated
```

> When `GAP_METRIC_SOURCE_ID` or `GAP_WEBHOOK_ID` are blank the tool calls the HyperDX API and picks the first result automatically. If multiple exist a warning is printed — supply the ID explicitly to avoid ambiguity.

---

## `migrate.py` — CLI flags

```
python migrate.py [options]

Options:
  --env-file FILE       Path to .env file (default: .env next to script)
  --config FILE         JSON config file (lowest priority)
  --dry-run             Preview only — nothing is written
  --skip-grafana        Skip dashboard migration
  --skip-alerts         Skip alert rule migration
  --backfill            Also backfill historical Prometheus data into ClickHouse
  --skip-validate       Skip post-migration validation

  # Override any .env value directly:
  --grafana-url URL
  --grafana-api-key KEY
  --prometheus-url URL
  --alertmanager-url URL
  --hyperdx-url URL
  --hyperdx-api-key KEY
  --metric-source-id ID
  --webhook-id ID
```

---

## Pipeline Steps

`migrate.py` runs the `gap` binary in sequence:

| Step | Subcommand | Description |
|---|---|---|
| 1 | `fetch` | Pull dashboards, rules, and configs from GAP stack |
| 2 | `convert-grafana` | Convert dashboards → HyperDX format |
| 3 | `apply-grafana` | POST dashboards to HyperDX |
| 4 | `convert-alerts` | Convert Prometheus alert rules → HyperDX format |
| 5 | `apply-alerts` | POST alert dashboards + rules to HyperDX |
| 6 | `backfill` | *(optional)* Bulk-insert historical metrics into ClickHouse |
| 7 | `validate` | Side-by-side comparison of source vs HyperDX state |

---

## Build & Test

```bash
go build -o gap ./cmd/gap   # outputs gap.exe on Windows
go test ./...
```

> **Requires Go 1.22+**

---

## Project Layout

```
gap_tool/
├── migrate.py                    ← Python orchestrator (start here)
├── .env.example                  ← copy to .env and fill in values
├── cmd/gap/main.go               ← binary entrypoint
├── internal/gaptool/
│   ├── fetch_validate.go         ← fetch GAP configs, validate migration
│   ├── grafana_to_hyperdx.go     ← Grafana dashboards → HyperDX format
│   ├── promql_to_hyperdx.go      ← PromQL / alert rules → HyperDX format
│   ├── migration.go              ← apply dashboards & alerts to HyperDX
│   ├── prometheus_backfill.go    ← historical Prometheus → ClickHouse
│   ├── validation.go             ← compare source vs target state
│   ├── hyperdx_client.go         ← HyperDX REST API client
│   ├── autodiscover.go           ← auto-discover metric source ID & webhook ID
│   ├── split_cli.go              ← stdin → stdout JSON entrypoints
│   ├── split_requests.go         ← request struct definitions
│   └── helpers.go                ← shared type-conversion utilities
└── go.mod
```

---

## Advanced: Direct Binary Usage

Each subcommand reads a JSON request from `stdin` and writes a JSON response to `stdout`. Diagnostic logs go to `stderr`.

**Fetch GAP configuration:**
```bash
echo '{
  "grafana_url":      "http://grafana:3000",
  "grafana_api_key":  "glsa_...",
  "prometheus_url":   "http://prometheus:9090",
  "alertmanager_url": "http://alertmanager:9093"
}' | ./gap fetch > gap_data.json
```

**Convert + apply dashboards** (IDs auto-discovered when omitted):
```bash
# Auto-discover metric source ID from HyperDX
echo "{
  \"gap_data\": $(jq '.data' gap_data.json),
  \"hyperdx_url\": \"http://hyperdx:8080\",
  \"hyperdx_api_key\": \"YOUR_KEY\"
}" | ./gap convert-grafana > converted.json

echo "{
  \"hyperdx_url\": \"http://hyperdx:8080\",
  \"hyperdx_api_key\": \"YOUR_KEY\",
  \"dashboards\": $(jq '.dashboards' converted.json)
}" | ./gap apply-grafana
```

**Backfill historical metrics:**
```bash
echo '{
  "prometheus_url": "http://prometheus:9090",
  "lookback_days":  3,
  "dry_run":        false
}' | ./gap backfill
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `GAPTOOL_INSECURE_SKIP_VERIFY=true` | Skip TLS certificate verification |

---

## Error Handling

All subcommands output a JSON object on success and failure:

```json
{ "status": true,  "message": "..." }   // exit 0
{ "status": false, "message": "..." }   // exit 1
```

The `backfill` command additionally returns partial results on failure — check `stats.errors` for per-metric details.
