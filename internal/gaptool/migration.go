package gaptool

import "fmt"

func convertGrafanaPayload(req convertGrafanaRequest) map[string]any {
	dashboards := convertAllGrafanaDashboards(asMap(req.GapData["grafana"]), req.HyperDXMetricSourceID)
	return map[string]any{
		"status":          true,
		"message":         fmt.Sprintf("Converted %d Grafana dashboards to HyperDX payloads.", len(dashboards)),
		"dashboards":      dashboards,
		"dashboard_count": len(dashboards),
	}
}

func applyGrafanaDashboards(req applyGrafanaRequest) (map[string]any, error) {
	if req.DryRun {
		return map[string]any{
			"status":          true,
			"message":         fmt.Sprintf("Dry run complete. Prepared %d Grafana dashboards for HyperDX.", len(req.Dashboards)),
			"dashboard_count": len(req.Dashboards),
			"dry_run":         true,
		}, nil
	}

	client, err := newHyperDXClient(req.HyperDXURL, req.HyperDXAPIKey)
	if err != nil {
		return nil, err
	}

	for _, dashboardReq := range req.Dashboards {
		name := firstString(dashboardReq["name"])
		if _, err := client.createDashboard(dashboardReq); err != nil {
			return nil, fmt.Errorf("failed to create dashboard %q: %w", name, err)
		}
	}

	return map[string]any{
		"status":          true,
		"message":         fmt.Sprintf("Migrated %d Grafana dashboards to HyperDX.", len(req.Dashboards)),
		"dashboard_count": len(req.Dashboards),
	}, nil
}

func convertAlertsPayload(req convertAlertsRequest) map[string]any {
	alertPairs := convertPrometheusRulesToHyperDX(asMap(req.GapData["prometheus"]), req.HyperDXMetricSourceID, req.WebhookID)
	return map[string]any{
		"status":      true,
		"message":     fmt.Sprintf("Converted %d Prometheus alert rules to HyperDX payloads.", len(alertPairs)),
		"alert_pairs": alertPairs,
		"alert_count": len(alertPairs),
	}
}

func applyAlertPairs(req applyAlertsRequest) (map[string]any, error) {
	if req.DryRun {
		return map[string]any{
			"status":      true,
			"message":     fmt.Sprintf("Dry run complete. Prepared %d alert rules for HyperDX.", len(req.AlertPairs)),
			"alert_count": len(req.AlertPairs),
			"dry_run":     true,
		}, nil
	}

	client, err := newHyperDXClient(req.HyperDXURL, req.HyperDXAPIKey)
	if err != nil {
		return nil, err
	}

	for _, pair := range req.AlertPairs {
		createdDashboard, err := client.createDashboard(pair.Dashboard)
		if err != nil {
			return nil, fmt.Errorf("failed to create alert dashboard %q: %w", firstString(pair.Dashboard["name"]), err)
		}

		dashboardID, tileID, err := extractCreatedDashboardIdentifiers(client, createdDashboard)
		if err != nil {
			return nil, err
		}

		alertRequest := cloneMap(pair.Alert)
		delete(alertRequest, "_alert_name")
		alertRequest["dashboardId"] = dashboardID
		alertRequest["tileId"] = tileID

		if _, err := client.createAlert(alertRequest); err != nil {
			return nil, fmt.Errorf("failed to create alert %q: %w", firstString(alertRequest["name"]), err)
		}
	}

	return map[string]any{
		"status":      true,
		"message":     fmt.Sprintf("Migrated %d alert rules to HyperDX.", len(req.AlertPairs)),
		"alert_count": len(req.AlertPairs),
	}, nil
}

func extractCreatedDashboardIdentifiers(client *hyperDXClient, dashboard map[string]any) (string, string, error) {
	dashboardID := firstString(
		dashboard["id"],
		dashboard["_id"],
		asMap(dashboard["data"])["id"],
		asMap(dashboard["data"])["_id"],
	)
	if dashboardID == "" {
		return "", "", fmt.Errorf("created dashboard response did not include an id")
	}

	tileID := firstTileID(dashboard)
	if tileID == "" {
		fullDashboard, err := client.getDashboard(dashboardID)
		if err != nil {
			return "", "", fmt.Errorf("failed to fetch created dashboard %q for tile lookup: %w", dashboardID, err)
		}
		tileID = firstTileID(fullDashboard)
	}
	if tileID == "" {
		return "", "", fmt.Errorf("created dashboard %q did not expose a tile id", dashboardID)
	}

	return dashboardID, tileID, nil
}

func firstTileID(dashboard map[string]any) string {
	for _, container := range []map[string]any{dashboard, asMap(dashboard["data"])} {
		if len(container) == 0 {
			continue
		}
		for _, tile := range toMapSlice(container["tiles"]) {
			if tileID := firstString(tile["id"], tile["_id"]); tileID != "" {
				return tileID
			}
		}
	}
	return ""
}

func toAlertPairs(value any) []alertPair {
	if typed, ok := value.([]alertPair); ok {
		return typed
	}
	if typed, ok := value.(alertPair); ok {
		return []alertPair{typed}
	}

	items := asSlice(value)
	result := make([]alertPair, 0, len(items))
	for _, item := range items {
		mapped := asMap(item)
		pair := alertPair{
			Dashboard: asMap(mapped["dashboard"]),
			Alert:     asMap(mapped["alert"]),
		}
		if len(pair.Dashboard) > 0 || len(pair.Alert) > 0 {
			result = append(result, pair)
			continue
		}

		if typed, ok := item.(alertPair); ok {
			result = append(result, typed)
		}
	}
	return result
}
