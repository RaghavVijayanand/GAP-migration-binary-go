package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── resolveIDWithCredentials ──────────────────────────────────────────────────

func TestResolveIDWithCredentialsPassthroughWhenIDSet(t *testing.T) {
	called := false
	got, err := resolveIDWithCredentials("http://hdx", "key", "explicit-id",
		func(_ *hyperDXClient, id string) (string, error) {
			called = true
			return id, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "explicit-id" {
		t.Fatalf("expected explicit-id, got %q", got)
	}
	if called {
		t.Fatal("resolver should not be called when ID is already set")
	}
}

func TestResolveIDWithCredentialsSkipsWhenNoCredentials(t *testing.T) {
	called := false
	got, err := resolveIDWithCredentials("", "", "",
		func(_ *hyperDXClient, id string) (string, error) {
			called = true
			return "discovered", nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty id, got %q", got)
	}
	if called {
		t.Fatal("resolver should not be called when credentials are missing")
	}
}

// ── resolveMetricSourceID ─────────────────────────────────────────────────────

func TestResolveMetricSourceIDPassthrough(t *testing.T) {
	// Server should never be hit.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected HTTP call to %s", r.URL.Path)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	got, err := resolveMetricSourceID(client, "existing-id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "existing-id" {
		t.Fatalf("expected existing-id, got %q", got)
	}
}

func TestResolveMetricSourceIDAutoDiscovers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/api/v2/sources" {
			_, _ = w.Write([]byte(`[{"_id":"src-abc","name":"Metrics"}]`))
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	got, err := resolveMetricSourceID(client, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "src-abc" {
		t.Fatalf("expected src-abc, got %q", got)
	}
}

func TestResolveMetricSourceIDErrorsWhenNoneExist(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/api/v2/sources" {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	_, err := resolveMetricSourceID(client, "")
	if err == nil {
		t.Fatal("expected error when no sources exist")
	}
	if !strings.Contains(err.Error(), "no metric sources") {
		t.Fatalf("expected 'no metric sources' in error, got: %v", err)
	}
}

func TestResolveMetricSourceIDWarnsOnMultiple(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/api/v2/sources" {
			_, _ = w.Write([]byte(`[{"_id":"src-first"},{"_id":"src-second"}]`))
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	got, err := resolveMetricSourceID(client, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should pick the first.
	if got != "src-first" {
		t.Fatalf("expected src-first, got %q", got)
	}
}

// ── resolveWebhookID ──────────────────────────────────────────────────────────

func TestResolveWebhookIDPassthrough(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected HTTP call to %s", r.URL.Path)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	got, err := resolveWebhookID(client, "wh-existing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "wh-existing" {
		t.Fatalf("expected wh-existing, got %q", got)
	}
}

func TestResolveWebhookIDAutoDiscovers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/api/v2/webhooks" {
			_, _ = w.Write([]byte(`[{"_id":"wh-123","name":"Slack"}]`))
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	got, err := resolveWebhookID(client, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "wh-123" {
		t.Fatalf("expected wh-123, got %q", got)
	}
}

func TestResolveWebhookIDReturnsEmptyWhenNoneExist(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/api/v2/webhooks" {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	got, err := resolveWebhookID(client, "")
	if err != nil {
		t.Fatalf("expected no error when no webhooks exist, got: %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty webhook id, got %q", got)
	}
}

// ── End-to-end: runConvertGrafana and runConvertAlerts with auto-discovery ────

func TestRunConvertGrafanaAutoDiscoversSouceID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/api/v2/sources" {
			_, _ = w.Write([]byte(`[{"_id":"auto-src"}]`))
			return
		}
		t.Errorf("unexpected path: %s", r.URL.Path)
	}))
	defer server.Close()

	input, _ := json.Marshal(map[string]any{
		"hyperdx_url":     server.URL,
		"hyperdx_api_key": "key",
		"gap_data": map[string]any{
			"grafana": map[string]any{"dashboards": []any{}},
		},
		// hyperdx_metric_source_id intentionally omitted
	})

	payload, exitCode := runConvertGrafana(input)
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d: %v", exitCode, payload["message"])
	}
	if payload["status"] != true {
		t.Fatalf("expected status true, got %#v", payload)
	}
}

func TestRunConvertAlertsAutoDiscoversIDs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/api/v2/sources":
			_, _ = w.Write([]byte(`[{"_id":"auto-src"}]`))
		case "/api/api/v2/webhooks":
			_, _ = w.Write([]byte(`[{"_id":"auto-wh"}]`))
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	input, _ := json.Marshal(map[string]any{
		"hyperdx_url":     server.URL,
		"hyperdx_api_key": "key",
		"gap_data": map[string]any{
			"prometheus": map[string]any{
				"rules": map[string]any{"data": map[string]any{"groups": []any{}}},
			},
		},
		// both IDs omitted
	})

	payload, exitCode := runConvertAlerts(input)
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d: %v", exitCode, payload["message"])
	}
	if payload["status"] != true {
		t.Fatalf("expected status true, got %#v", payload)
	}
}
