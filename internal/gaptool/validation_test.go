package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ── collectSourceDashboards ───────────────────────────────────────────────────

func TestCollectSourceDashboards(t *testing.T) {
	data := map[string]any{
		"grafana": map[string]any{
			"dashboards": []any{
				map[string]any{"dashboard": map[string]any{"title": "dash-1", "panels": []any{1, 2}}},
				map[string]any{"dashboard": map[string]any{"title": "dash-2"}},
				map[string]any{"dashboard": map[string]any{"title": ""}}, // skipped
			},
		},
	}
	res := collectSourceDashboards(data)
	if len(res) != 2 {
		t.Fatalf("expected 2 dashboards, got %d", len(res))
	}
	if res["dash-1"].(map[string]any)["panel_count"] != 2 {
		t.Fatalf("expected 2 panels for dash-1")
	}
}

// ── collectSourceAlerts ───────────────────────────────────────────────────────

func TestCollectSourceAlerts(t *testing.T) {
	data := map[string]any{
		"prometheus": map[string]any{
			"rules": map[string]any{
				"data": map[string]any{
					"groups": []any{
						map[string]any{
							"rules": []any{
								map[string]any{"type": "alerting", "name": "alert1", "expr": "sum(x)"},
								map[string]any{"type": "record", "name": "rec1"}, // skipped
								map[string]any{"type": "alerting"},               // named missing, skipped
							},
						},
					},
				},
			},
		},
	}
	res := collectSourceAlerts(data)
	if len(res) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(res))
	}
	if res["alert1"].(map[string]any)["expr"] != "sum(x)" {
		t.Fatal("alert1 expr mismatch")
	}
}

// ── collectTargetDashboards ───────────────────────────────────────────────────

func TestCollectTargetDashboards(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/dashboards":
			json.NewEncoder(w).Encode(map[string]any{
				"data": []any{
					map[string]any{"name": "d1", "_id": "1", "tiles": []any{map[string]any{"id": "t1"}}},
					map[string]any{"name": "d2", "_id": "2"}, // tiles empty -> expects follow-up get
					map[string]any{"name": ""},               // missing name -> skipped
				},
			})
		case "/api/v2/dashboards/2":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "d2", "_id": "2", "tiles": []any{map[string]any{"id": "t1"}, map[string]any{"id": "t2"}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	res, err := collectTargetDashboards(client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 dashboards, got %d", len(res))
	}
	if res["d2"].(map[string]any)["tile_count"] != 2 {
		t.Fatalf("expected d2 to fetch 2 tiles, got %v", res)
	}
}

func TestCollectTargetDashboardsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client, _ := newHyperDXClient(server.URL, "key")
	_, err := collectTargetDashboards(client)
	if err == nil {
		t.Fatal("expected error on 500")
	}
}

// ── collectTargetAlerts ───────────────────────────────────────────────────────

func TestCollectTargetAlerts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"data": []any{
				map[string]any{"name": "a1"},
				map[string]any{"name": ""}, // skipped
			},
		})
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	res, err := collectTargetAlerts(client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(res))
	}
	if res["a1"].(map[string]any)["name"] != "a1" {
		t.Fatalf("expected a1")
	}
}

func TestCollectTargetAlertsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client, _ := newHyperDXClient(server.URL, "key")
	_, err := collectTargetAlerts(client)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── validateMigration (overall) ───────────────────────────────────────────────

func TestValidateMigrationSuccess(t *testing.T) {
	// Need mock servers for gap fetches and hyperdx client
	hdxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
	}))
	defer hdxServer.Close()

	gapServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mock responses for /api/search or /api/v1/rules
		json.NewEncoder(w).Encode(map[string]any{"status": "success"})
	}))
	defer gapServer.Close()

	req := validateRequest{
		GrafanaURL:      gapServer.URL,
		PrometheusURL:   gapServer.URL,
		AlertmanagerURL: gapServer.URL,
		HyperDXURL:      hdxServer.URL,
		HyperDXAPIKey:   "key",
	}

	// Will pass gap fetch without errors for this simple structure
	res, err := validateMigration(req)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res["status"] != true {
		t.Fatal("expected status=true")
	}
}
