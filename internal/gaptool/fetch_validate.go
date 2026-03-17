package gaptool

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type fetchRequest struct {
	GrafanaURL      string `json:"grafana_url"`
	PrometheusURL   string `json:"prometheus_url"`
	AlertmanagerURL string `json:"alertmanager_url"`
	GrafanaAPIKey   string `json:"grafana_api_key"`
}

type validateRequest struct {
	GrafanaURL      string `json:"grafana_url"`
	PrometheusURL   string `json:"prometheus_url"`
	AlertmanagerURL string `json:"alertmanager_url"`
	GrafanaAPIKey   string `json:"grafana_api_key"`
	HyperDXURL      string `json:"hyperdx_url"`
	HyperDXAPIKey   string `json:"hyperdx_api_key"`
}

func RunFetchCLI() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		writeJSON(map[string]any{"status": false, "message": err.Error()}, 1)
		return
	}

	payload, exitCode := runFetch(input)
	writeJSON(payload, exitCode)
}

func RunValidateCLI() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		writeJSON(validationFailurePayload(err.Error()), 1)
		return
	}

	payload, exitCode := runValidate(input)
	writeJSON(payload, exitCode)
}

func runFetch(input []byte) (map[string]any, int) {
	var req fetchRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return map[string]any{"status": false, "message": err.Error()}, 1
	}

	data, err := fetchAllGAPConfigs(req)
	if err != nil {
		return map[string]any{"status": false, "data": map[string]any{}, "message": err.Error()}, 1
	}

	return map[string]any{"status": true, "data": data, "message": "GAP configs fetched successfully."}, 0
}

func runValidate(input []byte) (map[string]any, int) {
	var req validateRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return validationFailurePayload(err.Error()), 1
	}

	result, err := validateMigration(req)
	if err != nil {
		return validationFailurePayload(err.Error()), 1
	}

	return result, 0
}

func validationFailurePayload(message string) map[string]any {
	return map[string]any{
		"status":            false,
		"message":           message,
		"source_dashboards": map[string]any{},
		"target_dashboards": map[string]any{},
		"source_alerts":     map[string]any{},
		"target_alerts":     map[string]any{},
	}
}

func writeJSON(payload map[string]any, exitCode int) {
	encoded, _ := json.Marshal(payload)
	fmt.Println(string(encoded))
	os.Exit(exitCode)
}

func newHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.EqualFold(os.Getenv("GAPTOOL_INSECURE_SKIP_VERIFY"), "true") {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	return &http.Client{Timeout: 10 * time.Second, Transport: transport}
}

func fetchJSON(client *http.Client, rawURL string, headers map[string]string) (any, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("request to %s failed with status %d", rawURL, resp.StatusCode)
	}

	var payload any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func fetchAllGAPConfigs(req fetchRequest) (map[string]any, error) {
	client := newHTTPClient()
	result := map[string]any{}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	setErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}

	wg.Add(3)
	go func() {
		defer wg.Done()
		data, err := extractPrometheus(client, req.PrometheusURL)
		setErr(err)
		mu.Lock()
		result["prometheus"] = data
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		data, err := extractAlertmanager(client, req.AlertmanagerURL)
		if err != nil {
			data = map[string]any{}
		}
		mu.Lock()
		result["alertmanager"] = data
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		data, err := extractGrafana(client, req.GrafanaURL, req.GrafanaAPIKey)
		setErr(err)
		mu.Lock()
		result["grafana"] = data
		mu.Unlock()
	}()
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return result, nil
}

func extractPrometheus(client *http.Client, baseURL string) (map[string]any, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("prometheus URL is required")
	}

	base := strings.TrimRight(baseURL, "/")
	result := map[string]any{}
	var mu sync.Mutex
	var firstErr error

	// setErr records the first error from a required endpoint.
	setErr := func(key string, err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = fmt.Errorf("[Prometheus] %s failed: %w", key, err)
		}
	}

	type endpoint struct {
		key      string
		path     string
		required bool
	}
	endpoints := []endpoint{
		{"config_raw", "/api/v1/status/config", true},
		{"rules", "/api/v1/rules", true},
		{"targets", "/api/v1/targets", false},
		{"metadata", "/api/v1/metadata", false},
		{"tsdb_stats", "/api/v1/status/tsdb", false},
	}

	var wg sync.WaitGroup
	for _, ep := range endpoints {
		wg.Add(1)
		go func(e endpoint) {
			defer wg.Done()
			data, err := fetchJSON(client, base+e.path, nil)
			if err != nil {
				if e.required {
					setErr(e.key, err)
				} else {
					fmt.Fprintf(os.Stderr, "[Prometheus] %s failed: %v\n", e.key, err)
				}
				return
			}
			mu.Lock()
			result[e.key] = data
			if e.key == "config_raw" {
				if typed, ok := data.(map[string]any); ok {
					if inner, ok := typed["data"].(map[string]any); ok {
						result["prometheus_yml"] = inner["yaml"]
					}
				}
			}
			mu.Unlock()
		}(ep)
	}

	// Instant-query metrics (all best-effort, fully concurrent)
	queries := map[string]string{
		"retention_bytes":      "prometheus_tsdb_retention_limit_bytes",
		"storage_blocks_bytes": "prometheus_tsdb_storage_blocks_bytes",
		"ingestion_rate":       "rate(prometheus_tsdb_head_samples_appended_total[5m])",
		"compaction_failures":  "prometheus_tsdb_compactions_failed_total",
		"scrape_failures":      "up == 0",
	}
	metrics := make(map[string]any, len(queries))
	var metricsMu sync.Mutex
	for key, query := range queries {
		wg.Add(1)
		go func(k, q string) {
			defer wg.Done()
			queryURL := base + "/api/v1/query?" + url.Values{"query": []string{q}}.Encode()
			data, err := fetchJSON(client, queryURL, nil)
			metricsMu.Lock()
			defer metricsMu.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Prometheus] metric %s failed: %v\n", k, err)
				metrics[k] = nil
				return
			}
			metrics[k] = data
		}(key, query)
	}

	wg.Wait()
	result["metrics"] = metrics

	if firstErr != nil {
		return nil, firstErr
	}
	fmt.Fprintf(os.Stderr, "[Prometheus] fetched %d resource types\n", len(result))
	return result, nil
}

func extractAlertmanager(client *http.Client, baseURL string) (map[string]any, error) {
	if strings.TrimSpace(baseURL) == "" {
		return map[string]any{}, nil
	}

	base := strings.TrimRight(baseURL, "/")
	result := map[string]any{}
	var mu sync.Mutex
	var wg sync.WaitGroup

	type endpoint struct {
		key  string
		path string
	}
	endpoints := []endpoint{
		{"status", "/api/v2/status"},
		{"alerts", "/api/v2/alerts"},
		{"receivers", "/api/v2/receivers"},
		{"silences", "/api/v2/silences"},
	}

	for _, ep := range endpoints {
		wg.Add(1)
		go func(e endpoint) {
			defer wg.Done()
			data, err := fetchJSON(client, base+e.path, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Alertmanager] %s failed: %v\n", e.key, err)
				return
			}
			mu.Lock()
			result[e.key] = data
			if e.key == "status" {
				if typed, ok := data.(map[string]any); ok {
					if config, ok := typed["config"].(map[string]any); ok {
						result["alertmanager_yml"] = config["original"]
					}
				}
			}
			mu.Unlock()
		}(ep)
	}

	wg.Wait()
	fmt.Fprintf(os.Stderr, "[Alertmanager] fetched %d resource types\n", len(result))
	return result, nil
}

// dashboardConcurrency caps parallel Grafana dashboard fetches to avoid
// overwhelming large instances.
const dashboardConcurrency = 10

func extractGrafana(client *http.Client, baseURL, apiKey string) (map[string]any, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("grafana URL is required")
	}

	base := strings.TrimRight(baseURL, "/")
	headers := map[string]string{"Authorization": "Bearer " + apiKey}
	result := map[string]any{}

	// Auth check — /api/org works for both user sessions and service account
	// tokens (glsa_…); /api/user only works for interactive user sessions.
	if _, err := fetchJSON(client, base+"/api/org", headers); err != nil {
		return nil, fmt.Errorf("grafana authentication failed: %w", err)
	}

	// Top-level metadata endpoints — fetch concurrently, all best-effort
	var mu sync.Mutex
	var wg sync.WaitGroup

	type metaEndpoint struct {
		key  string
		path string
	}
	metaEndpoints := []metaEndpoint{
		{"datasources", "/api/datasources"},
		{"plugins", "/api/plugins?enabled=1"},
		{"folders", "/api/folders"},
		{"orgs", "/api/orgs"},
	}
	for _, ep := range metaEndpoints {
		wg.Add(1)
		go func(e metaEndpoint) {
			defer wg.Done()
			data, err := fetchJSON(client, base+e.path, headers)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Grafana] %s failed: %v\n", e.key, err)
				return
			}
			mu.Lock()
			result[e.key] = data
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	// Dashboard list (sequential — must complete before parallel fetches)
	searchData, err := fetchJSON(client, base+"/api/search?type=dash-db", headers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Grafana] dashboard list failed: %v\n", err)
		return result, nil
	}

	entries, ok := searchData.([]any)
	if !ok {
		result["dashboard_list"] = searchData
		return result, nil
	}
	result["dashboard_list"] = entries

	// Fetch individual dashboards concurrently with a bounded semaphore
	fmt.Fprintf(os.Stderr, "[Grafana] fetching %d dashboards (max %d concurrent)\n", len(entries), dashboardConcurrency)
	sem := make(chan struct{}, dashboardConcurrency)
	dashboards := make([]any, len(entries))

	for i, entry := range entries {
		entryMap, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		uid, _ := entryMap["uid"].(string)
		if uid == "" {
			continue
		}
		wg.Add(1)
		go func(idx int, u string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			data, err := fetchJSON(client, base+"/api/dashboards/uid/"+u, headers)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[Grafana] dashboard %s failed: %v\n", u, err)
				return
			}
			mu.Lock()
			dashboards[idx] = data
			mu.Unlock()
		}(i, uid)
	}
	wg.Wait()

	// Compact out nil slots left by skipped/failed dashboards
	filtered := dashboards[:0]
	for _, d := range dashboards {
		if d != nil {
			filtered = append(filtered, d)
		}
	}
	result["dashboards"] = filtered
	fmt.Fprintf(os.Stderr, "[Grafana] fetched %d dashboards\n", len(filtered))
	return result, nil
}
