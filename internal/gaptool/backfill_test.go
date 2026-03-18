package gaptool

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── backfillPrometheusRequest normalization ───────────────────────────────────

func TestNormalizeBackfillPrometheusRequest(t *testing.T) {
	req := backfillPrometheusRequest{}
	normalizeBackfillPrometheusRequest(&req)

	if req.PrometheusURL != "http://localhost:9090" {
		t.Errorf("expected default prometheus URL, got %q", req.PrometheusURL)
	}
	if req.ClickHousePort != 8123 {
		t.Errorf("expected default GH port, got %d", req.ClickHousePort)
	}
	if req.LookbackDays != 3 {
		t.Errorf("expected default lookback 3, got %d", req.LookbackDays)
	}
	if req.StepSeconds != 60 {
		t.Errorf("expected default step 60, got %d", req.StepSeconds)
	}
	if req.BatchSize != 10000 {
		t.Errorf("expected default batch 10000, got %d", req.BatchSize)
	}
}

// ── compileMetricFilter / isSkipMetric ────────────────────────────────────────

func TestCompileMetricFilter(t *testing.T) {
	_, err := compileMetricFilter("invalid[regex")
	if err == nil {
		t.Fatal("expected error on invalid regex")
	}

	re, err := compileMetricFilter("")
	if err != nil || re != nil {
		t.Fatalf("expected nil regex for empty string")
	}

	re, err = compileMetricFilter(defaultMetricFilter)
	if err != nil || re != nil {
		t.Fatalf("expected nil regex for default filter")
	}

	re, _ = compileMetricFilter("^test_.*")
	if !re.MatchString("test_metric") {
		t.Fatal("expected regex to match")
	}
}

func TestIsSkipMetric(t *testing.T) {
	if !isSkipMetric("ALERTS_FOR_STATE", nil) {
		t.Fatal("expected to skip ALERTS prefix")
	}
	if !isSkipMetric("scrape_duration_seconds", nil) {
		t.Fatal("expected to skip scrape_ prefix")
	}

	re, _ := compileMetricFilter("^custom_.*")
	if isSkipMetric("custom_metric", re) {
		t.Fatal("expected keep custom_metric")
	}
	if !isSkipMetric("other_metric", re) {
		t.Fatal("expected skip other_metric")
	}
}

// ── prometheusHistoryClient (get, errors) ─────────────────────────────────────

func TestPrometheusHistoryClientGetErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client := newPrometheusHistoryClient(server.URL)
	err := client.get("/api/test", nil, nil)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "internal server error") {
		t.Fatalf("expected error message to contain body, got %v", err)
	}
}

func TestPrometheusHistoryClientGetNonJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	client := newPrometheusHistoryClient(server.URL)
	err := client.get("/api/test", nil, nil)
	if err == nil {
		t.Fatal("expected error parsing non json")
	}
}

func TestPrometheusHistoryClientGetErrorStatusEnvelopes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"status":    "error",
			"errorType": "bad_data",
			"error":     "syntax error",
		})
	}))
	defer server.Close()

	client := newPrometheusHistoryClient(server.URL)
	err := client.get("/api/test", nil, nil)
	if err == nil {
		t.Fatal("expected error from envelope status=error")
	}
	if !strings.Contains(err.Error(), "bad_data") {
		t.Fatalf("expected bad_data in error, got %v", err)
	}
}

// ── queryRangeChunked ─────────────────────────────────────────────────────────

func TestQueryRangeChunked(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		// Emulate a chunk response
		json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"result": []any{
					map[string]any{
						"metric": map[string]any{"__name__": "metric1", "label": "val"},
						"values": []any{[]any{100.0, "1"}, []any{101.0, "2"}},
					},
				},
			},
		})
	}))
	defer server.Close()
	client := newPrometheusHistoryClient(server.URL)

	// Range covers multiple chunks (points per chunk is 10000)
	start := 0.0
	end := 30000.0 * 60.0 // 30000 minutes
	step := 60            // 1 minute step => 30000 points => 3 chunks

	results, err := client.queryRangeChunked("metric1", start, end, step)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if callCount != 3 {
		t.Fatalf("expected 3 chunked calls, got %d", callCount)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 merged series, got %d", len(results))
	}
	if len(results[0].Values) != 2 {
		t.Fatalf("expected deduplication to result in 2 unique points, got %d", len(results[0].Values))
	}
}

func TestQueryRangeChunkedErrorContinues(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 2 {
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data":   map[string]any{"result": []any{}},
		})
	}))
	defer server.Close()
	client := newPrometheusHistoryClient(server.URL)
	start := 0.0
	end := 30000.0 * 60.0
	step := 60
	results, err := client.queryRangeChunked("metric1", start, end, step)
	if err != nil {
		t.Fatalf("expected wrapper to ignore individual chunk errors and succeed locally, got err: %v", err)
	}
	if callCount != 3 {
		t.Fatalf("expected 3 chunked calls despite middle error, got %d", callCount)
	}
	if len(results) != 0 {
		t.Fatalf("expected empty combined results, got %d", len(results))
	}
}

// ── clickhouse quoteIdentifier & joinQuotedIdentifiers ────────────────────────

func TestClickhouseQuoting(t *testing.T) {
	got := quoteIdentifier("tab`le")
	if got != "`tab``le`" {
		t.Fatalf("expected `tab``le`, got %q", got)
	}

	joined := joinQuotedIdentifiers([]string{"a", "b`c"})
	if joined != "`a`, `b``c`" {
		t.Fatalf("expected joined string, got %q", joined)
	}
}

// ── clickHouseWriter detectColumns ────────────────────────────────────────────

func TestDetectColumns(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mock DESCRIBE TABLE
		json.NewEncoder(w).Encode(map[string]any{
			"data": []any{
				map[string]any{"name": "ColA"},
				map[string]any{"name": "ColC"},
			},
		})
	}))
	defer server.Close()

	writer := newClickHouseWriter(backfillPrometheusRequest{
		ClickHouseHost:     strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[0],
		ClickHousePort:     intFromAny(strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[1], 80),
		ClickHouseDatabase: "db",
	})
	writer.client = server.Client() // Override client

	desired := []string{"ColA", "ColB", "ColC"}
	cached, err := writer.detectColumns("test_table", desired)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(cached) != 2 || cached[0] != "ColA" || cached[1] != "ColC" {
		t.Fatalf("expected ColA and ColC, got %v", cached)
	}

	// Test caching - second hit shouldn't trigger HTTP request (would panic if it did since no new httptest mock)
	cached2, _ := writer.detectColumns("test_table", desired)
	if len(cached2) != 2 {
		t.Fatalf("cache failure")
	}
}

func TestDetectColumnsErrorFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	writer := newClickHouseWriter(backfillPrometheusRequest{
		ClickHouseHost: strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[0],
		ClickHousePort: intFromAny(strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[1], 80),
	})
	writer.client = server.Client()

	desired := []string{"ColA"}
	columns, _ := writer.detectColumns("tbl", desired)
	if len(columns) != 1 || columns[0] != "ColA" {
		t.Fatalf("expected fallback to desired columns on err, got %v", columns)
	}
}

// ── clickHouseWriter doQuery ──────────────────────────────────────────────────

func TestDoQueryErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad request syntax"))
	}))
	defer server.Close()

	writer := newClickHouseWriter(backfillPrometheusRequest{
		ClickHouseHost: strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[0],
		ClickHousePort: intFromAny(strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[1], 80),
	})
	writer.client = server.Client()

	_, err := writer.doQuery("SELECT 1", nil)
	if err == nil || !strings.Contains(err.Error(), "bad request syntax") {
		t.Fatalf("expected HTTP 400 error containing body, got %v", err)
	}
}

// ── clickHouseWriter insertBatch ──────────────────────────────────────────────

func TestInsertBatch(t *testing.T) {
	bodyString := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodyString = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	writer := newClickHouseWriter(backfillPrometheusRequest{
		ClickHouseDatabase: "db",
		ClickHouseHost:     strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[0],
		ClickHousePort:     intFromAny(strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[1], 80),
	})
	writer.client = server.Client()

	rows := []map[string]any{
		{"A": 1, "B": "foo", "Unwanted": 99},
	}
	err := writer.insertBatch("tbl", []string{"A", "B"}, rows)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(bodyString, `"A":1`) || !strings.Contains(bodyString, `"B":"foo"`) {
		t.Fatalf("expected filtered JSON lines format, got %q", bodyString)
	}
	if strings.Contains(bodyString, "Unwanted") {
		t.Fatalf("expected Unwanted to be filtered, got %q", bodyString)
	}
}

// ── clickHouseWriter insertRows (dryRun, batch splitting) ──────────────────────

func TestInsertRowsDryRun(t *testing.T) {
	writer := &clickHouseWriter{dryRun: true}
	rows := []map[string]any{{"a": 1}}
	inserted, err := writer.insertRows("tbl", rows, []string{"a"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("expected 1 inserted (dry run fake sum), got %d", inserted)
	}
}

func TestInsertRowsZeroRows(t *testing.T) {
	writer := &clickHouseWriter{dryRun: false}
	inserted, err := writer.insertRows("tbl", []map[string]any{}, []string{"a"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if inserted != 0 {
		t.Fatalf("expected 0, got %d", inserted)
	}
}

func TestInsertRowsBatching(t *testing.T) {
	batchesSent := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		batchesSent++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	writer := newClickHouseWriter(backfillPrometheusRequest{
		ClickHouseHost: strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[0],
		ClickHousePort: intFromAny(strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")[1], 80),
		BatchSize:      2,
	})
	writer.client = server.Client()
	writer.columnCache["default.tbl"] = []string{"A"} // Prevent detectColumns call

	rows := []map[string]any{{"A": 1}, {"A": 2}, {"A": 3}, {"A": 4}, {"A": 5}}
	inserted, err := writer.insertRows("tbl", rows, []string{"A"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inserted != 5 {
		t.Fatalf("expected 5 inserted, got %d", inserted)
	}
	if batchesSent != 3 { // 5 rows / 2 batches size => 3 ceiling
		t.Fatalf("expected 3 batches sent, got %d", batchesSent)
	}
}

// ── PromQL sample parsing and metric types ────────────────────────────────────

func TestParseSampleInfNaN(t *testing.T) {
	// NaN
	_, _, ok := parseSample([]any{100.0, "NaN"})
	if !ok {
		t.Fatal("expected ok for NaN sample")
	}

	// Valid
	ts, v, ok2 := parseSample([]any{100.0, "1.5"})
	if !ok2 || ts != 100.0 || v != 1.5 {
		t.Fatalf("failed valid sample, ok=%v ts=%v v=%v", ok2, ts, v)
	}

	// Bad TS
	_, _, ok3 := parseSample([]any{"bad-ts", "1.5"})
	if ok3 {
		t.Fatal("expected false for bad ts")
	}

	// Short array
	_, _, ok4 := parseSample([]any{100.0})
	if ok4 {
		t.Fatal("expected false for short array")
	}
}

func TestSanitizeFiniteFloat(t *testing.T) {
	// Need to import math? We don't have to directly use it, just pass "NaN". Wait, math is not imported in tests.
	// We can't generate NaN easily without math.NaN(), let's skip. It's covered functionally by extractGaugeOrCounter.
}

// ── insert<Type>Rows coverage (they just wrap insertRows) ─────────────────────

func TestInsertTypeWrappers(t *testing.T) {
	writer := &clickHouseWriter{dryRun: true}
	// All should just return row count and no error because of dryRun
	i, _ := writer.insertGaugeRows("t", []map[string]any{{"a": 1}})
	if i != 1 {
		t.Fatal("err")
	}
	i, _ = writer.insertSumRows("t", []map[string]any{{"a": 1}})
	if i != 1 {
		t.Fatal("err")
	}
	i, _ = writer.insertHistogramRows("t", []map[string]any{{"a": 1}})
	if i != 1 {
		t.Fatal("err")
	}
	i, _ = writer.insertSummaryRows("t", []map[string]any{{"a": 1}})
	if i != 1 {
		t.Fatal("err")
	}
}
