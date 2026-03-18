package gaptool

import (
	"fmt"
	"strings"
)

var panelTypeMap = map[string]string{
	"graph":      "line",
	"timeseries": "line",
	"stat":       "number",
	"singlestat": "number",
	"gauge":      "number",
	"bargauge":   "number",
	"table":      "table",
	"table-old":  "table",
	"piechart":   "pie",
	"text":       "markdown",
	"news":       "markdown",
	"logs":       "search",
	"alertlist":  "search",
}

func convertAllGrafanaDashboards(grafanaData map[string]any, sourceID string) []map[string]any {
	dashboards := asSlice(grafanaData["dashboards"])
	result := make([]map[string]any, 0, len(dashboards))
	for _, raw := range dashboards {
		envelope := asMap(raw)
		if len(envelope) == 0 {
			continue
		}
		converted, err := grafanaDashboardToHyperDX(envelope, sourceID)
		if err == nil {
			result = append(result, converted)
		}
	}
	return result
}

func grafanaDashboardToHyperDX(envelope map[string]any, sourceID string) (map[string]any, error) {
	dashboard := asMap(envelope["dashboard"])
	if len(dashboard) == 0 {
		dashboard = envelope
	}

	name := firstString(dashboard["title"])
	if name == "" {
		name = "Migrated Dashboard"
	}

	flatPanels := make([]map[string]any, 0)
	for _, rawPanel := range asSlice(dashboard["panels"]) {
		panel := asMap(rawPanel)
		if len(panel) == 0 {
			continue
		}
		if normalizedKey(toString(panel["type"])) == "row" {
			for _, nested := range asSlice(panel["panels"]) {
				nestedPanel := asMap(nested)
				if len(nestedPanel) > 0 {
					flatPanels = append(flatPanels, nestedPanel)
				}
			}
			continue
		}
		flatPanels = append(flatPanels, panel)
	}

	tiles := make([]any, 0, len(flatPanels))
	for index, panel := range flatPanels {
		tile := panelToTile(panel, sourceID, index)
		if len(tile) > 0 {
			tiles = append(tiles, tile)
		}
	}

	tags := make([]any, 0)
	for _, rawTag := range asSlice(dashboard["tags"]) {
		tag := firstString(rawTag)
		if tag == "" {
			continue
		}
		if len(tag) > 32 {
			tag = tag[:32]
		}
		tags = append(tags, tag)
		if len(tags) == 50 {
			break
		}
	}

	filters := make([]any, 0)
	templating := asMap(dashboard["templating"])
	for _, rawTpl := range asSlice(templating["list"]) {
		tpl := asMap(rawTpl)
		tplType := normalizedKey(toString(tpl["type"]))
		if tplType != "query" && tplType != "custom" && tplType != "constant" {
			continue
		}
		varName := firstString(tpl["name"])
		if varName == "" {
			continue
		}
		filters = append(filters, map[string]any{
			"type":       "QUERY_EXPRESSION",
			"name":       firstString(tpl["label"], varName),
			"expression": varName,
			"sourceId":   sourceID,
		})
	}

	request := map[string]any{"name": name, "tiles": tiles}
	if len(tags) > 0 {
		request["tags"] = tags
	}
	if len(filters) > 0 {
		request["filters"] = filters
	}
	return request, nil
}

func panelToTile(panel map[string]any, sourceID string, tileIndex int) map[string]any {
	panelType := normalizedKey(toString(panel["type"]))
	if panelType == "row" {
		return nil
	}

	title := firstString(panel["title"])
	if title == "" {
		title = fmt.Sprintf("Panel %d", tileIndex)
	}

	gridPos := asMap(panel["gridPos"])
	x := intFromAny(gridPos["x"], 0)
	y := intFromAny(gridPos["y"], tileIndex*3)
	w := intFromAny(gridPos["w"], 12)
	if w < 1 {
		w = 1
	}
	if w > 24 {
		w = 24
	}
	h := intFromAny(gridPos["h"], 3)
	if h < 1 {
		h = 3
	}

	displayType := panelTypeMap[panelType]
	if displayType == "" {
		displayType = "line"
	}

	if displayType == "markdown" {
		content := firstString(asMap(panel["options"])["content"], title)
		return map[string]any{
			"name":   title,
			"x":      x,
			"y":      y,
			"w":      w,
			"h":      h,
			"series": []any{map[string]any{"type": "markdown", "content": content}},
		}
	}

	if displayType == "search" {
		return map[string]any{
			"name": title,
			"x":    x,
			"y":    y,
			"w":    w,
			"h":    h,
			"series": []any{map[string]any{
				"type":          "search",
				"sourceId":      sourceID,
				"fields":        []any{},
				"where":         "",
				"whereLanguage": "lucene",
			}},
		}
	}

	targets := toMapSlice(panel["targets"])
	if len(targets) == 0 {
		if _, knownType := panelTypeMap[panelType]; !knownType {
			return map[string]any{
				"name": title,
				"x":    x,
				"y":    y,
				"w":    w,
				"h":    h,
				"series": []any{map[string]any{
					"type":           "time",
					"sourceId":       sourceID,
					"aggFn":          "count",
					"where":          "",
					"whereLanguage":  "sql",
					"groupBy":        []any{},
					"metricName":     "metric.value",
					"metricDataType": "gauge",
					"displayType":    displayType,
				}},
			}
		}
		return map[string]any{
			"name": title,
			"x":    x,
			"y":    y,
			"w":    w,
			"h":    h,
			"series": []any{map[string]any{
				"type":    "markdown",
				"content": fmt.Sprintf("**%s**\n\n_No data source targets found during migration._", title),
			}},
		}
	}

	seriesItems := make([]any, 0, len(targets))
	for _, target := range targets {
		expr := firstString(target["expr"], target["query"])
		if expr == "" {
			continue
		}
		legend := firstString(target["legendFormat"], target["alias"])
		item := promqlToSeries(expr, legend, sourceID)
		if labelWhere := extractLabelFilter(expr); labelWhere != "" {
			item["where"] = labelWhere
		}
		if legendGroupBy := extractLegendGroupBy(legend); len(legendGroupBy) > 0 {
			item["groupBy"] = legendGroupBy
		}
		seriesItems = append(seriesItems, item)
	}

	if len(seriesItems) == 0 {
		seriesItems = []any{map[string]any{
			"type":           "time",
			"sourceId":       sourceID,
			"aggFn":          "count",
			"where":          "",
			"whereLanguage":  "sql",
			"groupBy":        []any{},
			"metricName":     "metric.value",
			"metricDataType": "gauge",
			"displayType":    displayType,
		}}
	}

	if displayType == "line" && (panelType == "timeseries" || panelType == "graph") {
		displayType = grafanaDrawStyle(panel)
	}

	if displayType == "number" {
		seriesItems = seriesItems[:1]
		for _, raw := range seriesItems {
			series := asMap(raw)
			series["type"] = "number"
		}
	}

	if displayType == "pie" || displayType == "table" {
		seriesItems = seriesItems[:1]
		for _, raw := range seriesItems {
			series := asMap(raw)
			series["type"] = "table"
		}
	}

	if displayType == "line" || displayType == "stacked_bar" {
		for _, raw := range seriesItems {
			series := asMap(raw)
			series["displayType"] = displayType
		}
	}

	return map[string]any{
		"name":   title,
		"x":      x,
		"y":      y,
		"w":      w,
		"h":      h,
		"series": seriesItems,
	}
}

func grafanaDrawStyle(panel map[string]any) string {
	fieldConfig := asMap(panel["fieldConfig"])
	defaults := asMap(fieldConfig["defaults"])
	custom := asMap(defaults["custom"])
	stacking := asMap(custom["stacking"])
	if normalizedKey(toString(stacking["mode"])) == "normal" {
		return "stacked_bar"
	}
	if normalizedKey(toString(custom["drawStyle"])) == "bars" {
		return "stacked_bar"
	}
	return "line"
}

func inferMetricDataType(metricName, outerFn string) string {
	lowerMetricName := strings.ToLower(metricName)
	if strings.Contains(lowerMetricName, "_bucket") {
		return "histogram"
	}
	sumLikeFn := map[string]struct{}{
		"sum": {}, "count": {}, "rate": {}, "irate": {}, "increase": {}, "delta": {}, "count_over_time": {},
	}
	if strings.HasSuffix(lowerMetricName, "_total") {
		return "sum"
	}
	if _, ok := sumLikeFn[outerFn]; ok {
		return "sum"
	}
	return "gauge"
}
