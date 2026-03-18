package gaptool

import (
	"strings"
	"testing"
)

// ── grafanaDashboardToHyperDX & convertAllGrafanaDashboards ─────────────────

func TestConvertAllGrafanaDashboards(t *testing.T) {
	data := map[string]any{
		"dashboards": []any{
			map[string]any{
				"dashboard": map[string]any{
					"title": "A",
				},
			},
			map[string]any{}, // Empty envelope, gets skipped
		},
	}
	result := convertAllGrafanaDashboards(data, "src-1")
	if len(result) != 1 {
		t.Fatalf("expected 1 dashboard, got %d", len(result))
	}
}

func TestGrafanaDashboardToHyperDXDashboardName(t *testing.T) {
	// Nested under "dashboard" key
	res, err := grafanaDashboardToHyperDX(map[string]any{
		"dashboard": map[string]any{"title": "Custom Title"},
	}, "src-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res["name"] != "Custom Title" {
		t.Fatalf("expected Custom Title, got %v", res["name"])
	}

	// Direct (no envelope)
	res2, err := grafanaDashboardToHyperDX(map[string]any{
		"title": "Direct",
	}, "src-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res2["name"] != "Direct" {
		t.Fatalf("expected Direct, got %v", res2["name"])
	}

	// Missing title
	res3, err := grafanaDashboardToHyperDX(map[string]any{}, "src-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res3["name"] != "Migrated Dashboard" {
		t.Fatalf("expected Migrated Dashboard, got %v", res3["name"])
	}
}

func TestGrafanaDashboardToHyperDXRows(t *testing.T) {
	res, _ := grafanaDashboardToHyperDX(map[string]any{
		"panels": []any{
			map[string]any{
				"type": "row",
				"panels": []any{
					map[string]any{"title": "Nested 1", "type": "stat"},
					map[string]any{}, // empty panel
				},
			},
			map[string]any{"title": "TopLevel", "type": "timeseries"},
		},
	}, "src-1")
	tiles := toMapSlice(res["tiles"])
	if len(tiles) != 2 { // Nested 1 and TopLevel. The row itself is dropped.
		t.Fatalf("expected 2 tiles, got %d", len(tiles))
	}
}

func TestGrafanaDashboardToHyperDXTags(t *testing.T) {
	longTag := strings.Repeat("A", 40)
	res, _ := grafanaDashboardToHyperDX(map[string]any{
		"tags": []any{"valid", "", longTag},
	}, "src-1")
	tags := asSlice(res["tags"])
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags (empty string dropped), got %d", len(tags))
	}
	if tags[1].(string) != strings.Repeat("A", 32) {
		t.Fatalf("expected tag truncated to 32 chars")
	}
}

func TestGrafanaDashboardToHyperDXTemplating(t *testing.T) {
	res, _ := grafanaDashboardToHyperDX(map[string]any{
		"templating": map[string]any{
			"list": []any{
				map[string]any{"type": "query", "name": "var1", "label": "My Var"},
				map[string]any{"type": "datasource", "name": "skip"},
				map[string]any{"type": "custom", "name": "var2"}, // uses name as fallback label
				map[string]any{"type": "constant", "name": ""},   // skipped due to empty name
			},
		},
	}, "src-1")
	filters := toMapSlice(res["filters"])
	if len(filters) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(filters))
	}
	if filters[0]["name"] != "My Var" || filters[0]["expression"] != "var1" {
		t.Fatalf("filter 0 mismatch: %v", filters[0])
	}
	if filters[1]["name"] != "var2" || filters[1]["expression"] != "var2" {
		t.Fatalf("filter 1 mismatch: %v", filters[1])
	}
}

// ── panelToTile & grafanaDrawStyle ────────────────────────────────────────────

func TestPanelToTileUnknownTypeFallback(t *testing.T) {
	tile := panelToTile(map[string]any{"type": "unknown-magic-plugin"}, "src", 0)
	seriesList := toMapSlice(tile["series"])
	// Defaults to "line"
	if len(seriesList) > 0 {
		if seriesList[0]["displayType"] != "line" {
			t.Fatalf("expected displayType line fallback, got %v", seriesList[0]["displayType"])
		}
	}
}

func TestPanelToTileMarkdown(t *testing.T) {
	tile := panelToTile(map[string]any{"type": "text", "options": map[string]any{"content": "hello"}}, "src", 0)
	series := toMapSlice(tile["series"])[0]
	if series["type"] != "markdown" || series["content"] != "hello" {
		t.Fatalf("markdown mismatch: %v", series)
	}
}

func TestPanelToTileSearch(t *testing.T) {
	tile := panelToTile(map[string]any{"type": "logs"}, "src", 0)
	series := toMapSlice(tile["series"])[0]
	if series["type"] != "search" {
		t.Fatalf("search mismatch: %v", series)
	}
}

func TestPanelToTileNoTargets(t *testing.T) {
	tile := panelToTile(map[string]any{"type": "graph", "title": "Empty Graph"}, "src", 0)
	series := toMapSlice(tile["series"])[0]
	if series["type"] != "markdown" || !strings.Contains(toString(series["content"]), "No data source") {
		t.Fatalf("mismatch for no targets: %v", series)
	}
}

func TestPanelToTileTargetMissingExpr(t *testing.T) {
	tile := panelToTile(map[string]any{
		"type":    "graph",
		"targets": []any{map[string]any{"expr": ""}},
	}, "src", 0)
	series := toMapSlice(tile["series"])[0]
	if series["metricName"] != "metric.value" { // fallback for fully empty queries
		t.Fatalf("expected fallback series: %v", series)
	}
}

func TestPanelToTileGridPos(t *testing.T) {
	tile := panelToTile(map[string]any{
		"type":    "graph",
		"gridPos": map[string]any{"x": 1, "y": 2, "w": 0, "h": 0},
		"targets": []any{map[string]any{"expr": "foo"}},
	}, "src", 0)
	if tile["x"] != 1 || tile["y"] != 2 || tile["w"] != 1 || tile["h"] != 3 { // w defaults to 1 minimum, h to 3
		t.Fatalf("gridPos bounds check failed: %v", tile)
	}
}

func TestPanelToTilePieOrTable(t *testing.T) {
	tile := panelToTile(map[string]any{
		"type":    "piechart", // Maps to pie -> type: table
		"targets": []any{map[string]any{"expr": "a"}, map[string]any{"expr": "b"}},
	}, "src", 0)
	seriesList := toMapSlice(tile["series"])
	if len(seriesList) != 1 {
		t.Fatalf("expected pie to truncate to 1 series, got %d", len(seriesList))
	}
	if seriesList[0]["type"] != "table" {
		t.Fatalf("expected table type for pie, got %v", seriesList[0]["type"])
	}
}

func TestPanelToTileDrawStyleStackedBarParam(t *testing.T) {
	tile := panelToTile(map[string]any{
		"type": "timeseries",
		"fieldConfig": map[string]any{
			"defaults": map[string]any{
				"custom": map[string]any{
					"drawStyle": "bars",
				},
			},
		},
		"targets": []any{map[string]any{"expr": "a"}},
	}, "src", 0)
	series := toMapSlice(tile["series"])[0]
	if series["displayType"] != "stacked_bar" {
		t.Fatalf("expected stacked_bar, got %v", series["displayType"])
	}
}

// ── inferMetricDataType ───────────────────────────────────────────────────────

func TestInferMetricDataType(t *testing.T) {
	cases := []struct {
		metricName string
		outerFn    string
		want       string
	}{
		{"http_request_duration_bucket", "rate", "histogram"},
		{"http_requests_total", "sum", "sum"},
		{"node_cpu_seconds", "sum", "sum"},
		{"node_memory_MemFree", "avg", "gauge"},
		{"some_metric", "rate", "sum"},
	}
	for _, c := range cases {
		got := inferMetricDataType(c.metricName, c.outerFn)
		if got != c.want {
			t.Errorf("inferMetricDataType(%q, %q) = %q, want %q", c.metricName, c.outerFn, got, c.want)
		}
	}
}
