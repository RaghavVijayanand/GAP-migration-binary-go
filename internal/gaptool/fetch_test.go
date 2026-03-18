package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ── fetchJSON ─────────────────────────────────────────────────────────────────

func TestFetchJSONSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	client := newHTTPClient()
	res, err := fetchJSON(client, server.URL, map[string]string{"Authorization": "Bearer key"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := asMap(res)
	if m["ok"] != true {
		t.Fatalf("expected ok=true")
	}
}

func TestFetchJSONErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newHTTPClient()
	_, err := fetchJSON(client, server.URL, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── fetchAllGAPConfigs ────────────────────────────────────────────────────────

func TestFetchAllGAPConfigs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/search":
			json.NewEncoder(w).Encode([]any{map[string]any{"uid": "1", "type": "dash-db"}})
		case "/api/dashboards/uid/1":
			json.NewEncoder(w).Encode(map[string]any{"dashboard": map[string]any{"title": "dash1"}})
		case "/api/v1/rules":
			json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"groups": []any{}}})
		case "/api/v2/alerts": // Alertmanager
			json.NewEncoder(w).Encode([]any{})
		case "/api/v1/status/config": // Alertmanager config
			json.NewEncoder(w).Encode(map[string]any{"config": ""})
		}
	}))
	defer server.Close()

	req := fetchRequest{
		GrafanaURL:      server.URL,
		PrometheusURL:   server.URL,
		AlertmanagerURL: server.URL,
	}

	data, err := fetchAllGAPConfigs(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data["grafana"] == nil || data["prometheus"] == nil || data["alertmanager"] == nil {
		t.Fatalf("missing top level keys in gap data")
	}
}

// ── extract... ────────────────────────────────────────────────────────────────

func TestExtractAlertmanager(t *testing.T) {
	res, err := extractAlertmanager(newHTTPClient(), "http://invalid-url::||")
	if err == nil {
		t.Fatal("expected error on bad URL")
	}
	_ = res
}

func TestExtractPrometheus(t *testing.T) {
	res, err := extractPrometheus(newHTTPClient(), "http://invalid-url::||")
	if err == nil {
		t.Fatal("expected error on bad URL")
	}
	_ = res
}

func TestExtractGrafana(t *testing.T) {
	res, err := extractGrafana(newHTTPClient(), "http://invalid-url::||", "")
	if err == nil {
		t.Fatal("expected error on bad URL")
	}
	_ = res
}

// ── RunFetchCLI / runFetch ────────────────────────────────────────────────────

func TestRunFetchCLIinvalid(t *testing.T) {
	resp, code := runFetch([]byte("not json"))
	if code != 1 {
		t.Fatal("expected exit 1")
	}
	if resp["status"] != false {
		t.Fatal("expected status false")
	}
}

// ── RunValidateCLI / runValidate ──────────────────────────────────────────────

func TestRunValidateCLIinvalid(t *testing.T) {
	resp, code := runValidate([]byte("not json"))
	if code != 1 {
		t.Fatal("expected exit 1")
	}
	if resp["status"] != false {
		t.Fatal("expected status false")
	}
}
