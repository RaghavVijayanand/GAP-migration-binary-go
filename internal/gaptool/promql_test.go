package gaptool

import (
	"strings"
	"testing"
)

// ── extractLabelFilter ────────────────────────────────────────────────────────

func TestExtractLabelFilterEquality(t *testing.T) {
	got := extractLabelFilter(`http_requests_total{job="api",env="prod"}`)
	if !strings.Contains(got, "job = 'api'") {
		t.Fatalf("expected job = 'api', got %q", got)
	}
	if !strings.Contains(got, "env = 'prod'") {
		t.Fatalf("expected env = 'prod', got %q", got)
	}
}

func TestExtractLabelFilterNotEqual(t *testing.T) {
	got := extractLabelFilter(`http_requests_total{env!="dev"}`)
	if !strings.Contains(got, "env != 'dev'") {
		t.Fatalf("expected env != 'dev', got %q", got)
	}
}

func TestExtractLabelFilterRegexMatch(t *testing.T) {
	got := extractLabelFilter(`metric{job=~"api.*"}`)
	if !strings.Contains(got, "LIKE") {
		t.Fatalf("expected LIKE for =~, got %q", got)
	}
	if !strings.Contains(got, "api%") {
		t.Fatalf("expected .* → %%, got %q", got)
	}
}

func TestExtractLabelFilterRegexNotMatch(t *testing.T) {
	got := extractLabelFilter(`metric{job!~"test.*"}`)
	if !strings.Contains(got, "NOT LIKE") {
		t.Fatalf("expected NOT LIKE for !~, got %q", got)
	}
}

func TestExtractLabelFilterNoBraces(t *testing.T) {
	got := extractLabelFilter(`simple_metric`)
	if got != "" {
		t.Fatalf("expected empty string for no braces, got %q", got)
	}
}

func TestExtractLabelFilterMismatchedBraces(t *testing.T) {
	got := extractLabelFilter(`metric{unclosed`)
	if got != "" {
		t.Fatalf("expected empty for mismatched braces, got %q", got)
	}
}

func TestExtractLabelFilterEscapesSQLQuotes(t *testing.T) {
	got := extractLabelFilter(`metric{name="it's"}`)
	if !strings.Contains(got, "it''s") {
		t.Fatalf("expected SQL-escaped quotes, got %q", got)
	}
}

// ── extractLegendGroupBy ───────────────────────────────────────────────────────

func TestExtractLegendGroupByWithTemplate(t *testing.T) {
	got := extractLegendGroupBy("{{instance}}")
	if len(got) != 1 || got[0] != "instance" {
		t.Fatalf("expected [instance], got %v", got)
	}
}

func TestExtractLegendGroupByEmpty(t *testing.T) {
	got := extractLegendGroupBy("")
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestExtractLegendGroupByNoTemplate(t *testing.T) {
	got := extractLegendGroupBy("static label")
	if len(got) != 0 {
		t.Fatalf("expected empty for static label, got %v", got)
	}
}

func TestExtractLegendGroupByOnlyBraces(t *testing.T) {
	got := extractLegendGroupBy("{{   }}")
	if len(got) != 0 {
		t.Fatalf("expected empty for whitespace-only template, got %v", got)
	}
}

// ── extractQuantileLevel ──────────────────────────────────────────────────────

func TestExtractQuantileLevelHistogramQuantile(t *testing.T) {
	level, ok := extractQuantileLevel("histogram_quantile(0.95, rate(http_bucket[5m]))")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if level != 0.95 {
		t.Fatalf("expected 0.95, got %f", level)
	}
}

func TestExtractQuantileLevelQuantileFn(t *testing.T) {
	level, ok := extractQuantileLevel("quantile(0.99, cpu_usage)")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if level != 0.99 {
		t.Fatalf("expected 0.99, got %f", level)
	}
}

func TestExtractQuantileLevelNoMatch(t *testing.T) {
	_, ok := extractQuantileLevel("sum(rate(metric[5m]))")
	if ok {
		t.Fatal("expected ok=false for non-quantile expr")
	}
}

// ── parseDurationSeconds ──────────────────────────────────────────────────────

func TestParseDurationSecondsSeconds(t *testing.T) {
	if got := parseDurationSeconds("30s"); got != 30 {
		t.Fatalf("expected 30, got %d", got)
	}
}

func TestParseDurationSecondsMinutes(t *testing.T) {
	if got := parseDurationSeconds("5m"); got != 300 {
		t.Fatalf("expected 300, got %d", got)
	}
}

func TestParseDurationSecondsHours(t *testing.T) {
	if got := parseDurationSeconds("2h"); got != 7200 {
		t.Fatalf("expected 7200, got %d", got)
	}
}

func TestParseDurationSecondsDays(t *testing.T) {
	if got := parseDurationSeconds("1d"); got != 86400 {
		t.Fatalf("expected 86400, got %d", got)
	}
}

func TestParseDurationSecondsWeeks(t *testing.T) {
	if got := parseDurationSeconds("1w"); got != 604800 {
		t.Fatalf("expected 604800, got %d", got)
	}
}

func TestParseDurationSecondsEmpty(t *testing.T) {
	// Empty/invalid returns default of 60
	if got := parseDurationSeconds(""); got != 60 {
		t.Fatalf("expected 60 for empty, got %d", got)
	}
}

func TestParseDurationSecondsCompound(t *testing.T) {
	if got := parseDurationSeconds("1h30m"); got != 5400 {
		t.Fatalf("expected 5400 for 1h30m, got %d", got)
	}
}

// ── nearestHDXInterval ────────────────────────────────────────────────────────

func TestNearestHDXIntervalSmall(t *testing.T) {
	if got := nearestHDXInterval("30s"); got != "1m" {
		t.Fatalf("expected 1m, got %q", got)
	}
}

func TestNearestHDXIntervalExact(t *testing.T) {
	if got := nearestHDXInterval("5m"); got != "5m" {
		t.Fatalf("expected 5m, got %q", got)
	}
}

func TestNearestHDXIntervalLarge(t *testing.T) {
	if got := nearestHDXInterval("2d"); got != "1d" {
		t.Fatalf("expected 1d for >1d, got %q", got)
	}
}

// ── extractThreshold ──────────────────────────────────────────────────────────

func TestExtractThresholdAbove(t *testing.T) {
	v, typ := extractThreshold("cpu_usage > 80")
	if v == nil || *v != 80 {
		t.Fatalf("expected threshold=80, got %v", v)
	}
	if typ != "above" {
		t.Fatalf("expected above, got %q", typ)
	}
}

func TestExtractThresholdBelow(t *testing.T) {
	v, typ := extractThreshold("memory_free < 100")
	if v == nil || *v != 100 {
		t.Fatalf("expected threshold=100, got %v", v)
	}
	if typ != "below" {
		t.Fatalf("expected below, got %q", typ)
	}
}

func TestExtractThresholdLTE(t *testing.T) {
	v, typ := extractThreshold("metric <= 50")
	if v == nil || *v != 50 {
		t.Fatalf("expected 50, got %v", v)
	}
	if typ != "below" {
		t.Fatalf("expected below, got %q", typ)
	}
}

func TestExtractThresholdGTE(t *testing.T) {
	v, typ := extractThreshold("metric >= 10")
	if v == nil || *v != 10 {
		t.Fatalf("expected 10, got %v", v)
	}
	if typ != "above" {
		t.Fatalf("expected above, got %q", typ)
	}
}

func TestExtractThresholdNoComparison(t *testing.T) {
	v, typ := extractThreshold("sum(rate(metric[5m]))")
	if v != nil {
		t.Fatalf("expected nil threshold for no comparison, got %v", v)
	}
	if typ != "above" {
		t.Fatalf("expected default above, got %q", typ)
	}
}

// ── promqlToSeries ────────────────────────────────────────────────────────────

func TestPromqlToSeriesCounterTotal(t *testing.T) {
	series := promqlToSeries("http_requests_total", "", "src-1")
	if series["metricDataType"] != "sum" {
		t.Fatalf("expected sum for _total metric, got %v", series["metricDataType"])
	}
}

func TestPromqlToSeriesHistogramBucket(t *testing.T) {
	series := promqlToSeries("http_duration_bucket", "", "src-1")
	if series["metricDataType"] != "histogram" {
		t.Fatalf("expected histogram for _bucket metric, got %v", series["metricDataType"])
	}
}

func TestPromqlToSeriesWithQuantile(t *testing.T) {
	series := promqlToSeries("histogram_quantile(0.95, rate(http_bucket[5m]))", "", "src-1")
	if series["level"] == nil {
		t.Fatal("expected level field for quantile expr")
	}
}

func TestPromqlToSeriesUnknownAggDefaultsAvg(t *testing.T) {
	series := promqlToSeries("some_gauge", "", "src-1")
	if series["aggFn"] != "avg" {
		t.Fatalf("expected avg default, got %v", series["aggFn"])
	}
}

func TestPromqlToSeriesWithLegendGroupBy(t *testing.T) {
	series := promqlToSeries("cpu_usage", "{{instance}}", "src-1")
	groupBy, ok := series["groupBy"].([]any)
	if !ok || len(groupBy) == 0 {
		t.Fatalf("expected groupBy from legend, got %v", series["groupBy"])
	}
}

// ── extractMetricName ─────────────────────────────────────────────────────────

func TestExtractMetricNameFromSelector(t *testing.T) {
	got := extractMetricName("sum(rate(http_requests_total[5m])) by (job)")
	if got != "http_requests_total" {
		t.Fatalf("expected http_requests_total, got %q", got)
	}
}

func TestExtractMetricNameFallback(t *testing.T) {
	got := extractMetricName("sum(rate(avg(max(count(min())))))")
	if got != "metric.value" {
		t.Fatalf("expected metric.value fallback, got %q", got)
	}
}

// ── convertPrometheusRulesToHyperDX ──────────────────────────────────────────

func TestConvertPrometheusRulesSkipsNonAlertingRules(t *testing.T) {
	data := map[string]any{
		"rules": map[string]any{
			"data": map[string]any{
				"groups": []any{
					map[string]any{
						"rules": []any{
							map[string]any{"type": "record", "name": "rec", "query": "sum(x)"},
							map[string]any{"type": "alerting", "name": "HighCPU", "query": "cpu > 80"},
						},
					},
				},
			},
		},
	}
	pairs := convertPrometheusRulesToHyperDX(data, "src", "wh")
	if len(pairs) != 1 {
		t.Fatalf("expected 1 alert pair (recording rule skipped), got %d", len(pairs))
	}
}

func TestConvertPrometheusRulesSkipsMissingNameOrExpr(t *testing.T) {
	data := map[string]any{
		"rules": map[string]any{
			"data": map[string]any{
				"groups": []any{
					map[string]any{
						"rules": []any{
							map[string]any{"type": "alerting", "name": "", "query": "cpu > 80"},
							map[string]any{"type": "alerting", "name": "NoExpr", "query": ""},
						},
					},
				},
			},
		},
	}
	pairs := convertPrometheusRulesToHyperDX(data, "src", "wh")
	if len(pairs) != 0 {
		t.Fatalf("expected 0 valid pairs, got %d", len(pairs))
	}
}

func TestConvertPrometheusRulesDurationSuffix(t *testing.T) {
	data := map[string]any{
		"rules": map[string]any{
			"data": map[string]any{
				"groups": []any{
					map[string]any{
						"rules": []any{
							map[string]any{"type": "alerting", "name": "Test", "query": "x > 1", "duration": "120"},
						},
					},
				},
			},
		},
	}
	pairs := convertPrometheusRulesToHyperDX(data, "src", "wh")
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
}

// ── buildAlertPair ────────────────────────────────────────────────────────────

func TestBuildAlertPairWithSeverityLabel(t *testing.T) {
	pair := buildAlertPair("HighCPU", "cpu > 90", "5m",
		map[string]any{"severity": "critical"},
		map[string]any{"summary": "CPU is high", "description": "Node is overloaded"},
		"src", "wh",
	)
	tags := pair.Dashboard["tags"].([]any)
	found := false
	for _, t := range tags {
		if t == "severity:critical" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected severity:critical tag, got %v", tags)
	}
}

func TestBuildAlertPairNoThresholdDefaultsToOne(t *testing.T) {
	pair := buildAlertPair("Alert", "sum(rate(x[5m]))", "1m",
		map[string]any{}, map[string]any{},
		"src", "wh",
	)
	if pair.Alert["threshold"] != 1.0 {
		t.Fatalf("expected threshold=1.0, got %v", pair.Alert["threshold"])
	}
}

func TestBuildAlertPairMessageCombinesSummaryAndDescription(t *testing.T) {
	pair := buildAlertPair("Alert", "x > 1", "1m",
		map[string]any{},
		map[string]any{"summary": "Summary", "description": "Detail"},
		"src", "wh",
	)
	msg := pair.Alert["message"].(string)
	if !strings.Contains(msg, "Summary") || !strings.Contains(msg, "Detail") {
		t.Fatalf("expected combined message, got %q", msg)
	}
}

func TestBuildAlertPairIncludesChannelWhenWebhookSet(t *testing.T) {
	pair := buildAlertPair("Alert", "x > 1", "1m",
		map[string]any{}, map[string]any{},
		"src", "wh-123",
	)
	if pair.Alert["channel"] == nil {
		t.Fatal("expected channel block when webhook ID is provided")
	}
	channelMap := pair.Alert["channel"].(map[string]any)
	if channelMap["webhookId"] != "wh-123" {
		t.Fatalf("expected webhookId wh-123, got %v", channelMap["webhookId"])
	}
}

func TestBuildAlertPairOmitsChannelWhenWebhookEmpty(t *testing.T) {
	pair := buildAlertPair("Alert", "x > 1", "1m",
		map[string]any{}, map[string]any{},
		"src", "",
	)
	if pair.Alert["channel"] != nil {
		t.Fatalf("expected channel block to be omitted when webhook ID is empty, got %v", pair.Alert["channel"])
	}
}
