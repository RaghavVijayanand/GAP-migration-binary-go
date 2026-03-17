package main

import (
	"fmt"
	"os"

	"gap_tool/internal/gaptool"
)

const usage = `gap — GAP (Grafana / Alertmanager / Prometheus) → HyperDX / ClickHouse migration tool

Usage:
  gap <subcommand>

All subcommands read a JSON request from stdin and write a JSON response to stdout.

Subcommands:
  fetch             Fetch all configs from Grafana, Prometheus, and Alertmanager.
  validate          Compare source (GAP) state with target (HyperDX) state.
  convert-grafana   Convert Grafana dashboards to HyperDX format (no network writes).
  apply-grafana     Convert and push Grafana dashboards to HyperDX.
  convert-alerts    Convert Prometheus alert rules to HyperDX format (no network writes).
  apply-alerts      Convert and push alert rules to HyperDX.
  backfill          Backfill historical Prometheus metrics into ClickHouse.

Environment variables:
  GAPTOOL_INSECURE_SKIP_VERIFY  Set to "true" to skip TLS certificate verification.

Examples:
  echo '{"grafana_url":"http://grafana:3000","grafana_api_key":"glsa_...","prometheus_url":"http://prometheus:9090","alertmanager_url":"http://alertmanager:9093"}' | gap fetch
  echo '{"gap_data":<output of fetch>,"hyperdx_metric_source_id":"<id>"}' | gap convert-grafana
  echo '{"hyperdx_url":"http://hyperdx","hyperdx_api_key":"<key>","dashboards":<output of convert-grafana>}' | gap apply-grafana
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "fetch":
		gaptool.RunFetchCLI()
	case "validate":
		gaptool.RunValidateCLI()
	case "convert-grafana":
		gaptool.RunConvertGrafanaCLI()
	case "apply-grafana":
		gaptool.RunApplyGrafanaCLI()
	case "convert-alerts":
		gaptool.RunConvertAlertsCLI()
	case "apply-alerts":
		gaptool.RunApplyAlertsCLI()
	case "backfill":
		gaptool.RunHistoricalBackfillCLI()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", os.Args[1], usage)
		os.Exit(1)
	}
}
