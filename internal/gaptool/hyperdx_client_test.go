package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ── requestJSON ───────────────────────────────────────────────────────────────

func TestHyperDXClientRequestJSONInvalidPayload(t *testing.T) {
	client, _ := newHyperDXClient("http://localhost", "key")
	// Channels cannot be marshaled to JSON
	_, err := client.createAlert(map[string]any{"bad": make(chan int)})
	if err == nil {
		t.Fatal("expected error on invalid payload marshal")
	}
}

func TestHyperDXClientRequestJSONBadURL(t *testing.T) {
	client, _ := newHyperDXClient("http://[fe80::1%en0]/", "key")
	_, err := client.listDashboards()
	if err == nil {
		t.Fatal("expected error on bad URL")
	}
}

func TestHyperDXClientRequestJSONErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	_, err := client.listDashboards()
	if err == nil {
		t.Fatal("expected error on 500 status")
	}
}

func TestHyperDXClientRequestJSONSetsAuthorizationAndAPIKeyHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer key" {
			t.Fatalf("expected Authorization header, got %q", got)
		}
		if got := r.Header.Get("X-API-Key"); got != "key" {
			t.Fatalf("expected X-API-Key header, got %q", got)
		}
		json.NewEncoder(w).Encode(map[string]any{"results": []any{}})
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	if _, err := client.listDashboards(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHyperDXClientRequestJSONEmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	// createDashboard uses requestJSON directly
	resp, err := client.createDashboard(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("expected empty map, got %v", resp)
	}
}

func TestHyperDXClientRequestJSONInvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not-json"))
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	_, err := client.listDashboards()
	if err == nil {
		t.Fatal("expected error on unmarshal failure")
	}
}

func TestHyperDXClientRequestJSONRetriesOnRateLimit(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("rate limit exceeded"))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"id": "success"})
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	resp, err := client.createDashboard(map[string]any{"name": "test"})
	if err != nil {
		t.Fatalf("unexpected error after retries: %v", err)
	}
	if resp["id"] != "success" {
		t.Fatalf("expected success, got %v", resp["id"])
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestHyperDXClientRequestJSONMaxRetriesReached(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("service down"))
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")
	_, err := client.listDashboards()
	if err == nil {
		t.Fatal("expected error when max retries are exhausted")
	}
	if attempts != 4 { // 1 initial + 3 retries
		t.Fatalf("expected 4 attempts, got %d", attempts)
	}
}

// ── listDashboards / getDashboard / createDashboard ───────────────────────────

func TestHyperDXClientDashboards(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/dashboards":
			if r.Method == http.MethodGet {
				json.NewEncoder(w).Encode(map[string]any{"results": []string{"d1", "d2"}})
			} else if r.Method == http.MethodPost {
				json.NewEncoder(w).Encode(map[string]any{"id": "new-dash"})
			}
		case "/api/v2/dashboards/123":
			json.NewEncoder(w).Encode(map[string]any{"id": "123"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")

	list, err := client.listDashboards()
	if err != nil || len(list) != 2 {
		t.Fatalf("listDashboards err=%v, count=%d", err, len(list))
	}

	get, err := client.getDashboard("123")
	if err != nil || get["id"] != "123" {
		t.Fatalf("getDashboard err=%v, id=%v", err, get["id"])
	}

	create, err := client.createDashboard(map[string]any{"name": "test"})
	if err != nil || create["id"] != "new-dash" {
		t.Fatalf("createDashboard err=%v, id=%v", err, create["id"])
	}
}

// ── listAlerts / createAlert ──────────────────────────────────────────────────

func TestHyperDXClientAlerts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/alerts":
			if r.Method == http.MethodGet {
				json.NewEncoder(w).Encode(map[string]any{"items": []string{"a1"}})
			} else if r.Method == http.MethodPost {
				json.NewEncoder(w).Encode(map[string]any{"id": "new-alert"})
			}
		}
	}))
	defer server.Close()

	client, _ := newHyperDXClient(server.URL, "key")

	list, err := client.listAlerts()
	if err != nil || len(list) != 1 {
		t.Fatalf("listAlerts err=%v, count=%d", err, len(list))
	}

	create, err := client.createAlert(map[string]any{"name": "alert"})
	if err != nil || create["id"] != "new-alert" {
		t.Fatalf("createAlert err=%v, id=%v", err, create["id"])
	}
}
