package gaptool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func RunConvertGrafanaCLI() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		writeJSON(map[string]any{"status": false, "message": err.Error(), "dashboards": []any{}}, 1)
		return
	}
	payload, exitCode := runConvertGrafana(input)
	writeJSON(payload, exitCode)
}

func RunApplyGrafanaCLI() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		writeJSON(map[string]any{"status": false, "message": err.Error(), "dashboard_count": 0}, 1)
		return
	}
	payload, exitCode := runApplyGrafana(input)
	writeJSON(payload, exitCode)
}

func RunConvertAlertsCLI() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		writeJSON(map[string]any{"status": false, "message": err.Error(), "alert_pairs": []any{}}, 1)
		return
	}
	payload, exitCode := runConvertAlerts(input)
	writeJSON(payload, exitCode)
}

func RunApplyAlertsCLI() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		writeJSON(map[string]any{"status": false, "message": err.Error(), "alert_count": 0}, 1)
		return
	}
	payload, exitCode := runApplyAlerts(input)
	writeJSON(payload, exitCode)
}

func RunHistoricalBackfillCLI() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		writeJSON(map[string]any{"status": false, "message": err.Error(), "stats": map[string]any{}}, 1)
		return
	}
	payload, exitCode := runHistoricalBackfill(input)
	writeJSON(payload, exitCode)
}

func runConvertGrafana(input []byte) (map[string]any, int) {
	var req convertGrafanaRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return map[string]any{"status": false, "message": err.Error(), "dashboards": []any{}}, 1
	}

	sourceID, err := resolveIDWithCredentials(
		req.HyperDXURL, req.HyperDXAPIKey,
		req.HyperDXMetricSourceID,
		func(c *hyperDXClient, id string) (string, error) { return resolveMetricSourceID(c, id) },
	)
	if err != nil {
		return map[string]any{"status": false, "message": err.Error(), "dashboards": []any{}}, 1
	}
	req.HyperDXMetricSourceID = sourceID

	return convertGrafanaPayload(req), 0
}

func runApplyGrafana(input []byte) (map[string]any, int) {
	var req applyGrafanaRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return map[string]any{"status": false, "message": err.Error(), "dashboard_count": 0}, 1
	}
	result, err := applyGrafanaDashboards(req)
	if err != nil {
		return map[string]any{"status": false, "message": err.Error(), "dashboard_count": 0}, 1
	}
	return result, 0
}

func runConvertAlerts(input []byte) (map[string]any, int) {
	var req convertAlertsRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return map[string]any{"status": false, "message": err.Error(), "alert_pairs": []any{}}, 1
	}

	sourceID, err := resolveIDWithCredentials(
		req.HyperDXURL, req.HyperDXAPIKey,
		req.HyperDXMetricSourceID,
		func(c *hyperDXClient, id string) (string, error) { return resolveMetricSourceID(c, id) },
	)
	if err != nil {
		return map[string]any{"status": false, "message": err.Error(), "alert_pairs": []any{}}, 1
	}
	req.HyperDXMetricSourceID = sourceID

	webhookID, err := resolveIDWithCredentials(
		req.HyperDXURL, req.HyperDXAPIKey,
		req.WebhookID,
		func(c *hyperDXClient, id string) (string, error) { return resolveWebhookID(c, id) },
	)
	if err != nil {
		return map[string]any{"status": false, "message": err.Error(), "alert_pairs": []any{}}, 1
	}
	req.WebhookID = webhookID

	return convertAlertsPayload(req), 0
}

func runApplyAlerts(input []byte) (map[string]any, int) {
	var req applyAlertsRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return map[string]any{"status": false, "message": err.Error(), "alert_count": 0}, 1
	}
	result, err := applyAlertPairs(req)
	if err != nil {
		return map[string]any{"status": false, "message": err.Error(), "alert_count": 0}, 1
	}
	return result, 0
}

func runHistoricalBackfill(input []byte) (map[string]any, int) {
	req := defaultBackfillPrometheusRequest()
	if len(bytes.TrimSpace(input)) > 0 {
		if err := json.Unmarshal(input, &req); err != nil {
			return map[string]any{"status": false, "message": err.Error(), "stats": map[string]any{}}, 1
		}
	}
	normalizeBackfillPrometheusRequest(&req)
	stats, err := migrateHistoricalData(req)
	if err != nil {
		return map[string]any{"status": false, "message": err.Error(), "stats": stats}, 1
	}
	status := stats.MetricsFailed == 0
	message := fmt.Sprintf("Historical Prometheus backfill finished. Migrated %d metrics with %d failures.", stats.MetricsMigrated, stats.MetricsFailed)
	if req.DryRun {
		message = fmt.Sprintf("Historical Prometheus backfill dry run finished. Prepared %d metrics with %d failures.", stats.MetricsMigrated, stats.MetricsFailed)
	}
	exitCode := 0
	if !status {
		exitCode = 1
	}
	return map[string]any{"status": status, "message": message, "stats": stats}, exitCode
}
