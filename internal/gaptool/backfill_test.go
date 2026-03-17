package gaptool

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDefaultAndNormalizeBackfillPrometheusRequest(t *testing.T) {
	req := backfillPrometheusRequest{
		PrometheusURL:      " http://prometheus:9090/ ",
		ClickHouseHost:     " clickhouse ",
		ClickHouseDatabase: " observability ",
		ClickHouseUsername: " admin ",
		MetricFilter:       " ",
		ServiceName:        " ",
	}

	normalizeBackfillPrometheusRequest(&req)

	if req.PrometheusURL != "http://prometheus:9090" {
		t.Fatalf("unexpected prometheus url: %q", req.PrometheusURL)
	}
	if req.ClickHouseHost != "clickhouse" {
		t.Fatalf("unexpected clickhouse host: %q", req.ClickHouseHost)
	}
	if req.ClickHousePort != defaultClickHousePort {
		t.Fatalf("expected default clickhouse port, got %d", req.ClickHousePort)
	}
	if req.LookbackDays != defaultLookbackDays {
		t.Fatalf("expected default lookback days, got %d", req.LookbackDays)
	}
	if req.StepSeconds != defaultStepSeconds {
		t.Fatalf("expected default step seconds, got %d", req.StepSeconds)
	}
	if req.BatchSize != defaultBatchSize {
		t.Fatalf("expected default batch size, got %d", req.BatchSize)
	}
	if req.MetricFilter != defaultMetricFilter {
		t.Fatalf("expected default metric filter, got %q", req.MetricFilter)
	}
	if req.ServiceName != defaultServiceName {
		t.Fatalf("expected default service name, got %q", req.ServiceName)
	}
}

func TestMigrateHistoricalDataDryRunSupportsCounterHistogramAndSummary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/label/__name__/values":
			_, _ = w.Write([]byte(`{"status":"success","data":["http_requests_total","request_duration_seconds_bucket","request_duration_seconds_count","request_duration_seconds_sum","rpc_latency_seconds","rpc_latency_seconds_count","rpc_latency_seconds_sum","up","scrape_duration_seconds"]}`))
		case "/api/v1/metadata":
			_, _ = w.Write([]byte(`{"status":"success","data":{"http_requests_total":[{"type":"counter","help":"Total requests","unit":"requests"}],"request_duration_seconds":[{"type":"histogram","help":"Request duration","unit":"seconds"}],"rpc_latency_seconds":[{"type":"summary","help":"RPC latency","unit":"seconds"}]}}`))
		case "/api/v1/query_range":
			query := r.URL.Query().Get("query")
			_, _ = w.Write([]byte(prometheusRangeQueryPayload(query)))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	stats, err := migrateHistoricalData(backfillPrometheusRequest{
		PrometheusURL: server.URL,
		DryRun:        true,
		LookbackDays:  1,
		StepSeconds:   60,
		BatchSize:     2,
		ServiceName:   "test-prometheus",
	})
	if err != nil {
		t.Fatalf("migrateHistoricalData returned error: %v", err)
	}
	if stats.MetricsDiscovered != 3 {
		t.Fatalf("expected 3 metrics discovered, got %d", stats.MetricsDiscovered)
	}
	if stats.MetricsSkipped != 2 {
		t.Fatalf("expected 2 skipped metrics, got %d", stats.MetricsSkipped)
	}
	if stats.MetricsMigrated != 3 {
		t.Fatalf("expected 3 migrated metrics, got %d", stats.MetricsMigrated)
	}
	if stats.MetricsFailed != 0 {
		t.Fatalf("expected 0 failed metrics, got %d (%v)", stats.MetricsFailed, stats.Errors)
	}
	if stats.RowsInserted["otel_metrics_sum"] != 2 {
		t.Fatalf("expected 2 sum rows, got %d", stats.RowsInserted["otel_metrics_sum"])
	}
	if stats.RowsInserted["otel_metrics_histogram"] != 2 {
		t.Fatalf("expected 2 histogram rows, got %d", stats.RowsInserted["otel_metrics_histogram"])
	}
	if stats.RowsInserted["otel_metrics_summary"] != 2 {
		t.Fatalf("expected 2 summary rows, got %d", stats.RowsInserted["otel_metrics_summary"])
	}
}

func prometheusRangeQueryPayload(query string) string {
	switch query {
	case "http_requests_total":
		return `{"status":"success","data":{"result":[{"metric":{"__name__":"http_requests_total","job":"api"},"values":[[1,"5"],[2,"7"]]}]}}`
	case "request_duration_seconds_bucket":
		return `{"status":"success","data":{"result":[{"metric":{"__name__":"request_duration_seconds_bucket","job":"api","le":"0.5"},"values":[[1,"2"],[2,"4"]]},{"metric":{"__name__":"request_duration_seconds_bucket","job":"api","le":"1"},"values":[[1,"4"],[2,"7"]]},{"metric":{"__name__":"request_duration_seconds_bucket","job":"api","le":"+Inf"},"values":[[1,"5"],[2,"9"]]}]}}`
	case "request_duration_seconds_count":
		return `{"status":"success","data":{"result":[{"metric":{"__name__":"request_duration_seconds_count","job":"api"},"values":[[1,"5"],[2,"9"]]}]}}`
	case "request_duration_seconds_sum":
		return `{"status":"success","data":{"result":[{"metric":{"__name__":"request_duration_seconds_sum","job":"api"},"values":[[1,"3.5"],[2,"6.0"]]}]}}`
	case "rpc_latency_seconds":
		return `{"status":"success","data":{"result":[{"metric":{"__name__":"rpc_latency_seconds","job":"api","quantile":"0.5"},"values":[[1,"0.2"],[2,"0.3"]]},{"metric":{"__name__":"rpc_latency_seconds","job":"api","quantile":"0.99"},"values":[[1,"0.8"],[2,"1.1"]]}]}}`
	case "rpc_latency_seconds_count":
		return `{"status":"success","data":{"result":[{"metric":{"__name__":"rpc_latency_seconds_count","job":"api"},"values":[[1,"5"],[2,"8"]]}]}}`
	case "rpc_latency_seconds_sum":
		return `{"status":"success","data":{"result":[{"metric":{"__name__":"rpc_latency_seconds_sum","job":"api"},"values":[[1,"1.7"],[2,"2.8"]]}]}}`
	default:
		return fmt.Sprintf(`{"status":"success","data":{"result":[]},"query":%q}`, query)
	}
}
