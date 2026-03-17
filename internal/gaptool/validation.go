package gaptool

func validateMigration(req validateRequest) (map[string]any, error) {
	gapData, err := fetchAllGAPConfigs(fetchRequest{
		GrafanaURL:      req.GrafanaURL,
		PrometheusURL:   req.PrometheusURL,
		AlertmanagerURL: req.AlertmanagerURL,
		GrafanaAPIKey:   req.GrafanaAPIKey,
	})
	if err != nil {
		return nil, err
	}

	client, err := newHyperDXClient(req.HyperDXURL, req.HyperDXAPIKey)
	if err != nil {
		return nil, err
	}

	targetDashboards, err := collectTargetDashboards(client)
	if err != nil {
		return nil, err
	}
	targetAlerts, err := collectTargetAlerts(client)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"status":            true,
		"source_dashboards": collectSourceDashboards(gapData),
		"target_dashboards": targetDashboards,
		"source_alerts":     collectSourceAlerts(gapData),
		"target_alerts":     targetAlerts,
	}, nil
}

func collectSourceDashboards(gapData map[string]any) map[string]any {
	result := map[string]any{}
	for _, rawDashboard := range asSlice(asMap(gapData["grafana"])["dashboards"]) {
		dashboard := asMap(rawDashboard)
		inner := asMap(dashboard["dashboard"])
		title := firstString(inner["title"])
		if title == "" {
			continue
		}
		result[normalizedKey(title)] = map[string]any{
			"title":       title,
			"panel_count": len(asSlice(inner["panels"])),
		}
	}
	return result
}

func collectSourceAlerts(gapData map[string]any) map[string]any {
	result := map[string]any{}
	rules := asMap(asMap(gapData["prometheus"])["rules"])
	for _, rawGroup := range asSlice(asMap(rules["data"])["groups"]) {
		group := asMap(rawGroup)
		for _, rawRule := range asSlice(group["rules"]) {
			rule := asMap(rawRule)
			if normalizedKey(toString(rule["type"])) != "alerting" {
				continue
			}
			name := firstString(rule["name"], rule["alert"])
			if name == "" {
				continue
			}
			result[normalizedKey(name)] = map[string]any{
				"name": name,
				"expr": firstString(rule["query"], rule["expr"]),
			}
		}
	}
	return result
}

func collectTargetDashboards(client *hyperDXClient) (map[string]any, error) {
	result := map[string]any{}
	dashboards, err := client.listDashboards()
	if err != nil {
		return nil, err
	}
	for _, dashboard := range dashboards {
		name := firstString(dashboard["name"])
		if name == "" {
			continue
		}
		tileCount := len(toMapSlice(dashboard["tiles"]))
		dashboardID := firstString(dashboard["id"], dashboard["_id"])
		if tileCount == 0 && dashboardID != "" {
			if fullDashboard, err := client.getDashboard(dashboardID); err == nil {
				tileCount = len(toMapSlice(fullDashboard["tiles"]))
			}
		}
		result[normalizedKey(name)] = map[string]any{"name": name, "tile_count": tileCount}
	}
	return result, nil
}

func collectTargetAlerts(client *hyperDXClient) (map[string]any, error) {
	result := map[string]any{}
	alerts, err := client.listAlerts()
	if err != nil {
		return nil, err
	}
	for _, alert := range alerts {
		name := firstString(alert["name"])
		if name == "" {
			continue
		}
		result[normalizedKey(name)] = map[string]any{"name": name}
	}
	return result, nil
}
