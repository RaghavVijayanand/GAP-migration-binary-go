package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ── convertGrafanaPayload ──────────────────────────────────────────────────────

func TestConvertGrafanaPayloadEmpty(t *testing.T) {
	req := convertGrafanaRequest{
		GapData:               map[string]any{"grafana": map[string]any{}},
		HyperDXMetricSourceID: "src-1",
	}
	resp := convertGrafanaPayload(req)
	if resp["status"] != true {
		t.Fatal("expected status=true")
	}
	if resp["dashboard_count"] != 0 {
		t.Fatalf("expected 0 dashboards, got %v", resp["dashboard_count"])
	}
}

// ── convertAlertsPayload ──────────────────────────────────────────────────────

func TestConvertAlertsPayloadEmpty(t *testing.T) {
	req := convertAlertsRequest{
		GapData: map[string]any{
			"prometheus": map[string]any{
				"rules": map[string]any{"data": map[string]any{"groups": []any{}}},
			},
		},
		HyperDXMetricSourceID: "src-1",
		WebhookID:             "wh-1",
	}
	resp := convertAlertsPayload(req)
	if resp["status"] != true {
		t.Fatal("expected status=true")
	}
	if resp["alert_count"] != 0 {
		t.Fatalf("expected 0 alerts, got %v", resp["alert_count"])
	}
}

// ── applyGrafanaDashboards (dry run) ──────────────────────────────────────────

func TestApplyGrafanaDashboardsDryRun(t *testing.T) {
	req := applyGrafanaRequest{
		DryRun:     true,
		Dashboards: []map[string]any{{"name": "Dash1"}, {"name": "Dash2"}},
	}
	resp, err := applyGrafanaDashboards(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp["status"] != true {
		t.Fatal("expected status=true in dry run")
	}
	if resp["dashboard_count"] != 2 {
		t.Fatalf("expected count=2, got %v", resp["dashboard_count"])
	}
	if resp["dry_run"] != true {
		t.Fatal("expected dry_run=true flag in response")
	}
}

// ── applyAlertPairs (dry run) ─────────────────────────────────────────────────

func TestApplyAlertPairsDryRun(t *testing.T) {
	req := applyAlertsRequest{
		DryRun: true,
		AlertPairs: []alertPair{
			{Dashboard: map[string]any{"name": "Alert1"}, Alert: map[string]any{"name": "Alert1"}},
		},
	}
	resp, err := applyAlertPairs(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp["alert_count"] != 1 {
		t.Fatalf("expected count=1, got %v", resp["alert_count"])
	}
	if resp["dry_run"] != true {
		t.Fatal("expected dry_run=true flag")
	}
}

// ── firstTileID ───────────────────────────────────────────────────────────────

func TestFirstTileIDFromTopLevel(t *testing.T) {
	dashboard := map[string]any{
		"id":    "dash-1",
		"tiles": []any{map[string]any{"id": "tile-1"}},
	}
	got := firstTileID(dashboard)
	if got != "tile-1" {
		t.Fatalf("expected tile-1, got %q", got)
	}
}

func TestFirstTileIDFromNestedData(t *testing.T) {
	dashboard := map[string]any{
		"data": map[string]any{
			"id":    "dash-1",
			"tiles": []any{map[string]any{"_id": "tile-99"}},
		},
	}
	got := firstTileID(dashboard)
	if got != "tile-99" {
		t.Fatalf("expected tile-99, got %q", got)
	}
}

func TestFirstTileIDEmpty(t *testing.T) {
	got := firstTileID(map[string]any{"id": "dash-1"})
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

// ── extractCreatedDashboardIdentifiers ────────────────────────────────────────

func TestExtractCreatedDashboardIdentifiersNoID(t *testing.T) {
	_, _, err := extractCreatedDashboardIdentifiers(nil, map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing id")
	}
}

func TestExtractCreatedDashboardIdentifiersWithTile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"_id":   "dash-1",
				"tiles": []any{map[string]any{"_id": "tile-1"}},
			},
		})
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "test-key")
	dashboard := map[string]any{
		"_id":   "dash-1",
		"tiles": []any{map[string]any{"id": "tile-42"}},
	}
	dashID, tileID, err := extractCreatedDashboardIdentifiers(client, dashboard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dashID != "dash-1" {
		t.Fatalf("expected dash-1, got %q", dashID)
	}
	if tileID != "tile-42" {
		t.Fatalf("expected tile-42, got %q", tileID)
	}
}

func TestExtractCreatedDashboardIdentifiersFromDataKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"_id":   "dash-2",
				"tiles": []any{map[string]any{"_id": "tile-2"}},
			},
		})
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	dashboard := map[string]any{
		"data": map[string]any{
			"id": "dash-2",
		},
	}
	dashID, tileID, err := extractCreatedDashboardIdentifiers(client, dashboard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dashID != "dash-2" {
		t.Fatalf("expected dash-2, got %q", dashID)
	}
	if tileID != "tile-2" {
		t.Fatalf("expected tile-2, got %q", tileID)
	}
}

// ── toAlertPairs ──────────────────────────────────────────────────────────────

func TestToAlertPairsTypedSlice(t *testing.T) {
	input := []alertPair{
		{Dashboard: map[string]any{"name": "D"}, Alert: map[string]any{"name": "A"}},
	}
	got := toAlertPairs(input)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
}

func TestToAlertPairsSingleItem(t *testing.T) {
	input := alertPair{
		Dashboard: map[string]any{"name": "D"},
		Alert:     map[string]any{"name": "A"},
	}
	got := toAlertPairs(input)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
}

func TestToAlertPairsFromAnySlice(t *testing.T) {
	input := []any{
		map[string]any{
			"dashboard": map[string]any{"name": "D"},
			"alert":     map[string]any{"name": "A"},
		},
	}
	got := toAlertPairs(input)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
}

func TestToAlertPairsEmptyDropsInvalid(t *testing.T) {
	input := []any{map[string]any{}}
	got := toAlertPairs(input)
	if len(got) != 0 {
		t.Fatalf("expected 0 (invalid entries dropped), got %d", len(got))
	}
}

// ── applyGrafanaDashboards (live create via mock) ─────────────────────────────

func TestApplyGrafanaDashboardsCreatesViaMockServer(t *testing.T) {
	created := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			created++
		}
		json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"_id": "dash-new"},
		})
	}))
	defer server.Close()

	req := applyGrafanaRequest{
		HyperDXURL:    server.URL,
		HyperDXAPIKey: "key",
		DryRun:        false,
		Dashboards:    []map[string]any{{"name": "Dash1"}, {"name": "Dash2"}},
	}
	resp, err := applyGrafanaDashboards(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if created != 2 {
		t.Fatalf("expected 2 POSTs, got %d", created)
	}
	if resp["dashboard_count"] != 2 {
		t.Fatalf("expected count=2, got %v", resp["dashboard_count"])
	}
}
