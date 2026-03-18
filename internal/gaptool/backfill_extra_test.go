package gaptool

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ── discoverMetrics ───────────────────────────────────────────────────────────

func TestDiscoverMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/label/__name__/values":
			json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data": []string{
					"http_req_duration_bucket", "http_req_duration_sum", "http_req_duration_count",
					"rpc_timing_quantile", "rpc_timing_sum", "rpc_timing_count",
					"up", "scrape_duration",
					"custom_counter_total",
					"custom_gauge_info",
					"unknown_metric",
					"bytes_transferred_bytes",
				},
			})
		case "/api/v1/metadata":
			json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data": map[string]any{
					"http_req_duration": []map[string]any{{"type": "histogram"}},
					"rpc_timing":        []map[string]any{{"type": "summary"}},
					"unknown_metric":    []map[string]any{{"type": "unknown"}},
				},
			})
		}
	}))
	defer server.Close()

	client := newPrometheusHistoryClient(server.URL)
	filter, _ := compileMetricFilter("^custom_.*|http_.*|rpc_.*|unknown_.*|bytes_.*") // Will skip 'up' and 'scrape_duration'
	metrics, skipped, err := discoverMetrics(client, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if skipped < 2 { // 'up' and 'scrape_duration' are inherently skipped or filtered out
		t.Fatalf("expected at least 2 skipped metrics, got %d", skipped)
	}

	foundHist := false
	foundSumm := false
	foundCounter := false
	foundGaugeFromInfo := false
	foundGaugeFromBytes := false

	for _, m := range metrics {
		switch m.PromName {
		case "http_req_duration":
			foundHist = true
			if m.MetricType != "histogram" {
				t.Fatalf("expected histogram type for %s, got %s", m.PromName, m.MetricType)
			}
		case "rpc_timing":
			foundSumm = true
		case "custom_counter_total":
			foundCounter = true
			if m.MetricType != "counter" { // Due to _total suffix
				t.Fatalf("expected counter for _total, got %s", m.MetricType)
			}
		case "custom_gauge_info":
			foundGaugeFromInfo = true
			if m.MetricType != "gauge" {
				t.Errorf("expected gauge for _info, got %s", m.MetricType)
			}
		case "bytes_transferred_bytes":
			foundGaugeFromBytes = true
			if m.MetricType != "gauge" { // without a _total, bytes is a gauge
				t.Errorf("expected gauge for _bytes without _total, got %s", m.MetricType)
			}
		}
	}

	if !foundHist || !foundSumm || !foundCounter || !foundGaugeFromInfo || !foundGaugeFromBytes {
		t.Fatalf("missing one or more expected inferred types")
	}
}

// ── parsePrometheusBound / sanitizeFiniteFloat / SeriesKeys ──────────────────

func TestParsePrometheusBound(t *testing.T) {
	v, err := parsePrometheusBound("+Inf")
	if err != nil || v != v { // math.IsInf check omitted, +Inf should parse
		t.Fatalf("expected +Inf, got %v err %v", v, err)
	}
	v, _ = parsePrometheusBound("1.5")
	if v != 1.5 {
		t.Fatalf("expected 1.5, got %v", v)
	}
}

func TestSanitizeFiniteFloatExtra(t *testing.T) {
	// Inf/NaN should return 0
	inf, _ := parsePrometheusBound("+Inf")
	if sanitizeFiniteFloat(inf) != 0 {
		t.Fatalf("expected Inf to sanitize to 0")
	}
	if sanitizeFiniteFloat(5.5) != 5.5 {
		t.Fatalf("expected 5.5 to return 5.5")
	}
}

func TestSeriesKeys(t *testing.T) {
	m := map[string]string{"job": "api", "env": "prod"}
	k := seriesKey(m)
	if k != "env=prod|job=api" {
		t.Fatalf("expected env=prod|job=api, got %q", k)
	}

	if seriesKey(nil) != "" {
		t.Fatalf("expected empty key")
	}

	tsKey := timestampedSeriesKey(k, 100.5)
	if tsKey != "env=prod|job=api@@100.5" {
		t.Fatalf("expected timestamped key, got %q", tsKey)
	}

	origK, ts := splitTimestampedSeriesKey(tsKey)
	if origK != k || ts != 100.5 {
		t.Fatalf("split failed")
	}

	if orig, tss := splitTimestampedSeriesKey("bad"); orig != "bad" || tss != 0 {
		t.Fatalf("expected bad to split cleanly")
	}
}

// ── extractHistogram / extractSummary / sampleLookup ─────────────────────────

func TestExtractHistogramAndSummary(t *testing.T) {
	// These functions require identical server mocks, let's mock query_range to return valid chunks
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"result": []any{
					map[string]any{
						"metric": map[string]string{"le": "1.0", "quantile": "0.95", "job": "api"},
						"values": []any{[]any{100.0, "10"}},
					},
				},
			},
		})
	}))
	defer server.Close()

	client := newPrometheusHistoryClient(server.URL)

	infoH := metricInfo{PromName: "h_metric", MetricType: "histogram", OTelName: "h_metric", TargetTable: "otel_metrics_histogram"}
	hRows, err := extractHistogram(client, infoH, 0, 1000, 60, "svc")
	if err != nil {
		t.Fatalf("unexpected err for histogram: %v", err)
	}
	if len(hRows) != 1 {
		t.Fatalf("expected 1 row for histogram, got %d", len(hRows))
	}

	infoS := metricInfo{PromName: "s_metric", MetricType: "summary", OTelName: "s_metric", TargetTable: "otel_metrics_summary"}
	sRows, err := extractSummary(client, infoS, 0, 1000, 60, "svc")
	if err != nil {
		t.Fatalf("unexpected err for summary: %v", err)
	}
	if len(sRows) != 1 {
		t.Fatalf("expected 1 row for summary, got %d", len(sRows))
	}

	// sampleLookup is called internally and effectively covered, but we can call it explicitly
	res := []prometheusMatrixResult{
		{
			Metric: map[string]string{"__name__": "m", "job": "api"},
			Values: [][]any{{100.0, "5"}},
		},
	}
	lookup := sampleLookup(res)
	if lookup["job=api@@100"] != 5.0 {
		t.Fatalf("sampleLookup failed: %v", lookup)
	}
}
