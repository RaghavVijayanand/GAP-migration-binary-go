package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewHTTPClientUsesSecureTLSByDefault(t *testing.T) {
	t.Setenv("GAPTOOL_INSECURE_SKIP_VERIFY", "")

	client := newHTTPClient()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}
	if transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("expected TLS verification to remain enabled by default")
	}
}

func TestNewHTTPClientAllowsExplicitInsecureOverride(t *testing.T) {
	t.Setenv("GAPTOOL_INSECURE_SKIP_VERIFY", "true")

	client := newHTTPClient()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}
	if transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("expected explicit insecure override to set InsecureSkipVerify")
	}
}

func TestExtractGrafanaUnauthorizedReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/org" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := extractGrafana(server.Client(), server.URL, "token")
	if err == nil {
		t.Fatal("expected extractGrafana to return an error on unauthorized")
	}
	if !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("expected authentication error, got: %v", err)
	}
}

func TestFetchAllGAPConfigsAggregatesPrometheusAlertmanagerAndGrafana(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/status/config":
			_, _ = w.Write([]byte(`{"data":{"yaml":"global: {}"}}`))
		case "/api/v1/rules":
			_, _ = w.Write([]byte(`{"data":{"groups":[{"rules":[{"alert":"HighCPU"}]}]}}`))
		case "/api/v1/targets":
			_, _ = w.Write([]byte(`{"data":{"activeTargets":[]}}`))
		case "/api/v1/metadata":
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case "/api/v1/status/tsdb":
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case "/api/v1/query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
		case "/api/v2/status":
			_, _ = w.Write([]byte(`{"config":{"original":"route:\n  receiver: default"}}`))
		case "/api/v2/alerts":
			_, _ = w.Write([]byte(`[]`))
		case "/api/v2/receivers":
			_, _ = w.Write([]byte(`[{"name":"default"}]`))
		case "/api/v2/silences":
			_, _ = w.Write([]byte(`[]`))
		case "/api/user", "/api/org":
			_, _ = w.Write([]byte(`{"login":"admin"}`))
		case "/api/datasources":
			_, _ = w.Write([]byte(`[]`))
		case "/api/plugins":
			_, _ = w.Write([]byte(`[]`))
		case "/api/folders":
			_, _ = w.Write([]byte(`[]`))
		case "/api/orgs":
			_, _ = w.Write([]byte(`[]`))
		case "/api/search":
			_, _ = w.Write([]byte(`[{"uid":"dash-1","title":"CPU"}]`))
		case "/api/dashboards/uid/dash-1":
			_, _ = w.Write([]byte(`{"dashboard":{"title":"CPU"},"meta":{}}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	result, err := fetchAllGAPConfigs(fetchRequest{
		GrafanaURL:      server.URL,
		PrometheusURL:   server.URL,
		AlertmanagerURL: server.URL,
		GrafanaAPIKey:   "token",
	})
	if err != nil {
		t.Fatalf("fetchAllGAPConfigs returned error: %v", err)
	}

	prometheus := result["prometheus"].(map[string]any)
	if got := prometheus["prometheus_yml"]; got != "global: {}" {
		t.Fatalf("unexpected prometheus yaml: %#v", got)
	}

	alertmanager := result["alertmanager"].(map[string]any)
	if got := alertmanager["alertmanager_yml"]; got != "route:\n  receiver: default" {
		t.Fatalf("unexpected alertmanager yaml: %#v", got)
	}

	grafana := result["grafana"].(map[string]any)
	dashboards := grafana["dashboards"].([]any)
	if len(dashboards) != 1 {
		t.Fatalf("expected 1 dashboard, got %d", len(dashboards))
	}
}

func TestExtractPrometheusFailsWhenRequiredRulesEndpointFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/status/config":
			_, _ = w.Write([]byte(`{"data":{"yaml":"global: {}"}}`))
		case "/api/v1/rules":
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			// non-critical concurrent requests — return 503 so they fail gracefully
			http.Error(w, "not configured", http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	_, err := extractPrometheus(server.Client(), server.URL)
	if err == nil {
		t.Fatal("expected extractPrometheus to fail when /api/v1/rules fails")
	}
	if !strings.Contains(err.Error(), "rules") {
		t.Fatalf("expected rules error, got %v", err)
	}
}

func TestFetchAllGAPConfigsTreatsAlertmanagerAsBestEffort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/status/config":
			_, _ = w.Write([]byte(`{"data":{"yaml":"global: {}"}}`))
		case "/api/v1/rules":
			_, _ = w.Write([]byte(`{"data":{"groups":[]}}`))
		case "/api/v1/targets":
			_, _ = w.Write([]byte(`{"data":{"activeTargets":[]}}`))
		case "/api/v1/metadata":
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case "/api/v1/status/tsdb":
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case "/api/v1/query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
		case "/api/v2/status":
			http.Error(w, "unavailable", http.StatusBadGateway)
		case "/api/v2/alerts", "/api/v2/receivers", "/api/v2/silences":
			// concurrent best-effort fetches — also unavailable
			http.Error(w, "unavailable", http.StatusBadGateway)
		case "/api/user", "/api/org":
			_, _ = w.Write([]byte(`{"login":"admin"}`))
		case "/api/datasources", "/api/plugins", "/api/folders", "/api/orgs":
			_, _ = w.Write([]byte(`[]`))
		case "/api/search":
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	result, err := fetchAllGAPConfigs(fetchRequest{
		GrafanaURL:      server.URL,
		PrometheusURL:   server.URL,
		AlertmanagerURL: server.URL,
		GrafanaAPIKey:   "token",
	})
	if err != nil {
		t.Fatalf("fetchAllGAPConfigs returned error: %v", err)
	}

	alertmanager := result["alertmanager"].(map[string]any)
	if len(alertmanager) != 0 {
		t.Fatalf("expected empty alertmanager result on best-effort failure, got %#v", alertmanager)
	}
	if _, ok := result["prometheus"].(map[string]any)["prometheus_yml"]; !ok {
		t.Fatalf("expected prometheus data to still be present, got %#v", result["prometheus"])
	}
}

func TestSplitDryRunReturnsCountsWithoutPostingToHyperDX(t *testing.T) {
	gapData := map[string]any{
		"grafana": map[string]any{
			"dashboards": []any{
				map[string]any{
					"dashboard": map[string]any{
						"title": "CPU",
						"panels": []any{
							map[string]any{
								"title":   "CPU Usage",
								"type":    "timeseries",
								"gridPos": map[string]any{"x": 0, "y": 0, "w": 12, "h": 8},
								"targets": []any{map[string]any{"expr": "sum(rate(cpu_usage_total[5m])) by (instance)"}},
							},
						},
					},
				},
			},
		},
		"prometheus": map[string]any{
			"rules": map[string]any{
				"data": map[string]any{
					"groups": []any{
						map[string]any{
							"rules": []any{
								map[string]any{
									"type":  "alerting",
									"name":  "HighCPU",
									"query": "sum(rate(cpu_usage_total[5m])) > 80",
								},
							},
						},
					},
				},
			},
		},
	}

	grafanaResult := convertGrafanaPayload(convertGrafanaRequest{
		GapData:               gapData,
		HyperDXMetricSourceID: "metric-source-id",
	})
	applyGrafanaResult, err := applyGrafanaDashboards(applyGrafanaRequest{
		HyperDXURL:    "http://hyperdx",
		HyperDXAPIKey: "api-key",
		Dashboards:    toMapSlice(grafanaResult["dashboards"]),
		DryRun:        true,
	})
	if err != nil {
		t.Fatalf("applyGrafanaDashboards returned error: %v", err)
	}

	alertsResult := convertAlertsPayload(convertAlertsRequest{
		GapData:               gapData,
		HyperDXMetricSourceID: "metric-source-id",
		WebhookID:             "webhook-id",
	})
	applyAlertsResult, err := applyAlertPairs(applyAlertsRequest{
		HyperDXURL:    "http://hyperdx",
		HyperDXAPIKey: "api-key",
		AlertPairs:    toAlertPairs(alertsResult["alert_pairs"]),
		DryRun:        true,
	})
	if err != nil {
		t.Fatalf("applyAlertPairs returned error: %v", err)
	}

	if grafanaResult["status"] != true || applyGrafanaResult["status"] != true {
		t.Fatalf("expected Grafana dry-run success, got %#v / %#v", grafanaResult, applyGrafanaResult)
	}
	if alertsResult["status"] != true || applyAlertsResult["status"] != true {
		t.Fatalf("expected alert dry-run success, got %#v / %#v", alertsResult, applyAlertsResult)
	}
	if got := applyGrafanaResult["dashboard_count"]; got != 1 {
		t.Fatalf("expected 1 dashboard, got %#v", got)
	}
	if got := applyAlertsResult["alert_count"]; got != 1 {
		t.Fatalf("expected 1 alert, got %#v", got)
	}
	if got := applyGrafanaResult["dry_run"]; got != true {
		t.Fatalf("expected dashboard dry_run=true, got %#v", got)
	}
	if got := applyAlertsResult["dry_run"]; got != true {
		t.Fatalf("expected alert dry_run=true, got %#v", got)
	}
}

func TestValidateMigrationFetchesSourceAndTargetData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/status/config":
			_, _ = w.Write([]byte(`{"data":{"yaml":"global: {}"}}`))
		case "/api/v1/rules":
			_, _ = w.Write([]byte(`{"data":{"groups":[{"rules":[{"type":"alerting","name":"HighCPU","query":"up == 0"}]}]}}`))
		case "/api/v1/targets", "/api/v1/metadata", "/api/v1/status/tsdb":
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case "/api/v1/query":
			_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
		case "/api/v2/status":
			_, _ = w.Write([]byte(`{"config":{"original":"route:\n  receiver: default"}}`))
		case "/api/v2/alerts", "/api/v2/receivers", "/api/v2/silences":
			if r.URL.Path == "/api/v2/alerts" && r.Header.Get("Authorization") != "" {
				// HyperDX Alerts
				json.NewEncoder(w).Encode(map[string]any{
					"data": []any{map[string]any{"name": "HighCPU"}},
				})
			} else {
				// Alertmanager (alerts, receivers, silences)
				_, _ = w.Write([]byte(`[]`))
			}
		case "/api/user", "/api/org":
			_, _ = w.Write([]byte(`{"login":"admin"}`))
		case "/api/datasources", "/api/plugins", "/api/folders", "/api/orgs":
			_, _ = w.Write([]byte(`[]`))
		case "/api/search":
			_, _ = w.Write([]byte(`[{"uid":"dash-1","title":"CPU"}]`))
		case "/api/dashboards/uid/dash-1":
			_, _ = w.Write([]byte(`{"dashboard":{"title":"CPU","panels":[{"title":"CPU Usage"}]},"meta":{}}`))
		case "/api/v2/dashboards":
			_, _ = w.Write([]byte(`[{"id":"dash-123","name":"CPU","tiles":[{"id":"tile-1"}]}]`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	result, err := validateMigration(validateRequest{
		GrafanaURL:      server.URL,
		PrometheusURL:   server.URL,
		AlertmanagerURL: server.URL,
		GrafanaAPIKey:   "token",
		HyperDXURL:      server.URL,
		HyperDXAPIKey:   "api-key",
	})
	if err != nil {
		t.Fatalf("validateMigration returned error: %v", err)
	}
	if result["status"] != true {
		t.Fatalf("expected success, got %#v", result)
	}

	sourceDashboards, ok := result["source_dashboards"].(map[string]any)
	if !ok {
		t.Fatalf("expected source_dashboards map, got %#v", result["source_dashboards"])
	}
	if _, ok := sourceDashboards["cpu"]; !ok {
		encoded, _ := json.Marshal(result)
		t.Fatalf("expected cpu source dashboard, got %s", string(encoded))
	}

	targetAlerts, ok := result["target_alerts"].(map[string]any)
	if !ok {
		t.Fatalf("expected target_alerts map, got %#v", result["target_alerts"])
	}
	if _, ok := targetAlerts["highcpu"]; !ok {
		encoded, _ := json.Marshal(result)
		t.Fatalf("expected highcpu target alert, got %s", string(encoded))
	}
}
