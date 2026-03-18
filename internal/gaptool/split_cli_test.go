package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ── runConvertGrafana ─────────────────────────────────────────────────────────

func TestRunConvertGrafanaInvalidJSON(t *testing.T) {
	resp, code := runConvertGrafana([]byte("not-json"))
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if resp["status"] != false {
		t.Fatal("expected status=false")
	}
}

func TestRunConvertGrafanaWithSourceID(t *testing.T) {
	input, _ := json.Marshal(map[string]any{
		"gap_data":                 map[string]any{"grafana": map[string]any{}},
		"hyperdx_metric_source_id": "src-explicit",
	})
	resp, code := runConvertGrafana(input)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d — %v", code, resp["message"])
	}
	if resp["status"] != true {
		t.Fatalf("expected status=true, got %v", resp["status"])
	}
}

func TestRunConvertGrafanaAutoDiscoverSourceID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"data": []any{map[string]any{"_id": "src-discovered"}},
		})
	}))
	defer server.Close()

	input, _ := json.Marshal(map[string]any{
		"gap_data":        map[string]any{"grafana": map[string]any{}},
		"hyperdx_url":     server.URL,
		"hyperdx_api_key": "key",
	})
	resp, code := runConvertGrafana(input)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d — %v", code, resp["message"])
	}
}

// ── runConvertAlerts ──────────────────────────────────────────────────────────

func TestRunConvertAlertsInvalidJSON(t *testing.T) {
	resp, code := runConvertAlerts([]byte("bad json"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}
	if resp["status"] != false {
		t.Fatal("expected status=false")
	}
}

func TestRunConvertAlertsWithExplicitIDs(t *testing.T) {
	input, _ := json.Marshal(map[string]any{
		"gap_data": map[string]any{
			"prometheus": map[string]any{
				"rules": map[string]any{"data": map[string]any{"groups": []any{}}},
			},
		},
		"hyperdx_metric_source_id": "src-1",
		"webhook_id":               "wh-1",
	})
	resp, code := runConvertAlerts(input)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d — %v", code, resp["message"])
	}
	if resp["alert_count"] != 0 {
		t.Fatalf("expected 0 alerts from empty groups, got %v", resp["alert_count"])
	}
}

func TestRunConvertAlertsDiscoversBothIDs(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		json.NewEncoder(w).Encode(map[string]any{
			"data": []any{map[string]any{"_id": "discovered-id"}},
		})
	}))
	defer server.Close()

	input, _ := json.Marshal(map[string]any{
		"gap_data": map[string]any{
			"prometheus": map[string]any{
				"rules": map[string]any{"data": map[string]any{"groups": []any{}}},
			},
		},
		"hyperdx_url":     server.URL,
		"hyperdx_api_key": "key",
	})
	resp, code := runConvertAlerts(input)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d — %v", code, resp["message"])
	}
	// Should have called /sources and /webhooks
	if callCount < 2 {
		t.Fatalf("expected at least 2 discovery calls, got %d", callCount)
	}
}

// ── runApplyGrafana ───────────────────────────────────────────────────────────

func TestRunApplyGrafanaInvalidJSON(t *testing.T) {
	resp, code := runApplyGrafana([]byte("bad"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}
	if resp["status"] != false {
		t.Fatal("expected status=false")
	}
}

func TestRunApplyGrafanaDryRun(t *testing.T) {
	input, _ := json.Marshal(map[string]any{
		"hyperdx_url":     "http://hyperdx",
		"hyperdx_api_key": "key",
		"dry_run":         true,
		"dashboards":      []any{map[string]any{"name": "D1"}},
	})
	resp, code := runApplyGrafana(input)
	if code != 0 {
		t.Fatalf("expected exit 0 in dry run, got %d", code)
	}
	if resp["dry_run"] != true {
		t.Fatal("expected dry_run=true in response")
	}
}

// ── runApplyAlerts ────────────────────────────────────────────────────────────

func TestRunApplyAlertsInvalidJSON(t *testing.T) {
	resp, code := runApplyAlerts([]byte("not-json"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}
	if resp["status"] != false {
		t.Fatal("expected status=false")
	}
}

func TestRunApplyAlertsDryRun(t *testing.T) {
	pairs := []alertPair{
		{Dashboard: map[string]any{"name": "D"}, Alert: map[string]any{"name": "A"}},
	}
	pairsJSON, _ := json.Marshal(pairs)

	input, _ := json.Marshal(map[string]any{
		"hyperdx_url":     "http://hyperdx",
		"hyperdx_api_key": "key",
		"dry_run":         true,
		"alert_pairs":     json.RawMessage(pairsJSON),
	})
	resp, code := runApplyAlerts(input)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d — %v", code, resp["message"])
	}
	if resp["dry_run"] != true {
		t.Fatal("expected dry_run=true")
	}
}

// ── runHistoricalBackfill ─────────────────────────────────────────────────────

func TestRunHistoricalBackfillInvalidJSON(t *testing.T) {
	resp, code := runHistoricalBackfill([]byte("not-json-at-all"))
	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}
	if resp["status"] != false {
		t.Fatal("expected status=false")
	}
}

func TestRunHistoricalBackfillEmptyInputUsesDefaults(t *testing.T) {
	// Empty JSON body is fine — uses all defaults.
	// Will fail to connect to localhost:9090, but we check graceful failure.
	resp, code := runHistoricalBackfill([]byte("{}"))
	// Expected to fail (no Prometheus), but must return JSON with stats.
	if resp["stats"] == nil {
		t.Fatal("expected stats key in response even on failure")
	}
	_ = code
}

func TestRunHistoricalBackfillBracesOnlyUsesDefaults(t *testing.T) {
	// whitespace-only triggers the bypass path
	resp, _ := runHistoricalBackfill([]byte("   "))
	if resp["stats"] == nil {
		t.Fatal("expected stats even for whitespace input")
	}
}

func TestRunHistoricalBackfillDryRunWithMockPrometheus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/label/__name__/values":
			json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data":   []string{"http_requests_total"},
			})
		case r.URL.Path == "/api/v1/metadata":
			json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data": map[string]any{
					"http_requests_total": []map[string]any{
						{"type": "counter", "help": "Total requests"},
					},
				},
			})
		case r.URL.Path == "/api/v1/query_range":
			json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data": map[string]any{
					"result": []any{
						map[string]any{
							"metric": map[string]string{"__name__": "http_requests_total"},
							"values": []any{[]any{1700000000.0, "42"}},
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	input, _ := json.Marshal(map[string]any{
		"prometheus_url": server.URL,
		"dry_run":        true,
		"lookback_days":  1,
		"metric_filter":  "",
	})
	resp, code := runHistoricalBackfill(input)
	if code != 0 {
		t.Fatalf("expected exit 0 for successful dry run, got %d — %v", code, resp["message"])
	}
	if resp["status"] != true {
		t.Fatalf("expected status=true, got %v", resp["status"])
	}
}
