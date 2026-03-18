package gaptool

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type alertPair struct {
	Dashboard map[string]any `json:"dashboard"`
	Alert     map[string]any `json:"alert"`
}

var promQLAggMap = map[string]string{
	"sum":                "sum",
	"avg":                "avg",
	"min":                "min",
	"max":                "max",
	"count":              "count",
	"rate":               "sum",
	"irate":              "sum",
	"increase":           "sum",
	"delta":              "sum",
	"topk":               "max",
	"bottomk":            "min",
	"quantile":           "quantile",
	"histogram_quantile": "quantile",
	"avg_over_time":      "avg",
	"sum_over_time":      "sum",
	"count_over_time":    "count",
	"last_over_time":     "last_value",
}

var supportedIntervals = []string{"1m", "5m", "15m", "30m", "1h", "6h", "12h", "1d"}

var intervalSeconds = map[string]int{
	"1m":  60,
	"5m":  300,
	"15m": 900,
	"30m": 1800,
	"1h":  3600,
	"6h":  21600,
	"12h": 43200,
	"1d":  86400,
}

var selectorPattern = regexp.MustCompile(`([A-Za-z_:][A-Za-z0-9_:]*)\s*(?:\{|\[)`)
var tokenPattern = regexp.MustCompile(`[A-Za-z_:][A-Za-z0-9_:]*`)
var outerPattern = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
var labelMatcherPattern = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|=|!=)\s*"([^"]*)"`)
var groupByPattern = regexp.MustCompile(`(?i)\b(?:by|without)\s*\(([^)]*)\)`)
var quantilePattern = regexp.MustCompile(`(?i)(?:histogram_quantile|quantile(?:_over_time)?)\s*\(\s*([0-9.]+)`)
var durationPattern = regexp.MustCompile(`(\d+)([smhdw])`)
var comparisonPattern = regexp.MustCompile(`([><!=]+)\s*([0-9.]+)\s*$`)

func promqlToSeries(expr, legend, sourceID string) map[string]any {
	metricName := extractMetricName(expr)
	outerFn := extractOuterFn(expr)
	aggFn := promQLAggMap[outerFn]
	metricDataType := inferMetricDataType(metricName, outerFn)
	if aggFn == "" {
		aggFn = "avg"
	}
	if outerFn == "" && metricDataType == "sum" {
		aggFn = "sum"
	}

	series := map[string]any{
		"type":           "time",
		"sourceId":       sourceID,
		"aggFn":          aggFn,
		"metricName":     metricName,
		"metricDataType": metricDataType,
		"where":          "",
		"whereLanguage":  "sql",
		"groupBy":        extractGroupBy(expr),
	}
	if aggFn == "quantile" {
		if level, ok := extractQuantileLevel(expr); ok {
			series["level"] = level
		} else {
			series["level"] = 0.95
		}
	}
	if legendGroupBy := extractLegendGroupBy(legend); len(legendGroupBy) > 0 {
		series["groupBy"] = legendGroupBy
	}
	return series
}

func convertPrometheusRulesToHyperDX(prometheusData map[string]any, sourceID, webhookID string) []alertPair {
	rulesPayload := asMap(prometheusData["rules"])
	groups := asSlice(asMap(rulesPayload["data"])["groups"])
	result := make([]alertPair, 0)
	for _, rawGroup := range groups {
		group := asMap(rawGroup)
		for _, rawRule := range asSlice(group["rules"]) {
			rule := asMap(rawRule)
			if normalizedKey(toString(rule["type"])) != "alerting" {
				continue
			}
			alertName := firstString(rule["name"], rule["alert"])
			expr := firstString(rule["query"], rule["expr"])
			if alertName == "" || expr == "" {
				continue
			}
			forDuration := firstString(rule["duration"])
			if forDuration == "" {
				forDuration = fmt.Sprintf("%ds", intFromAny(rule["duration"], 60))
			} else if !strings.ContainsAny(forDuration, "smhdw") {
				forDuration = forDuration + "s"
			}
			result = append(result, buildAlertPair(
				alertName,
				expr,
				forDuration,
				asMap(rule["labels"]),
				asMap(rule["annotations"]),
				sourceID,
				webhookID,
			))
		}
	}
	return result
}

func buildAlertPair(alertName, expr, forDuration string, labels, annotations map[string]any, sourceID, webhookID string) alertPair {
	threshold, thresholdType := extractThreshold(expr)
	if threshold == nil {
		defaultThreshold := 1.0
		threshold = &defaultThreshold
	}

	series := promqlToSeries(expr, "", sourceID)
	if labelWhere := extractLabelFilter(expr); labelWhere != "" {
		series["where"] = labelWhere
	}

	tile := map[string]any{
		"name":   fmt.Sprintf("%s – metric", alertName),
		"x":      0,
		"y":      0,
		"w":      24,
		"h":      4,
		"series": []any{series},
	}

	dashboardTags := []any{"migrated-alert"}
	if severity := firstString(labels["severity"]); severity != "" {
		dashboardTags = append(dashboardTags, "severity:"+severity)
	}

	summary := firstString(annotations["summary"], alertName)
	description := firstString(annotations["description"])
	message := summary
	if description != "" {
		message = summary + "\n\n" + description
	}

	alert := map[string]any{
		"_alert_name":   alertName,
		"name":          alertName,
		"message":       truncateString(message, 2000),
		"threshold":     *threshold,
		"thresholdType": thresholdType,
		"interval":      nearestHDXInterval(forDuration),
		"source":        "tile",
	}

	if webhookID != "" {
		alert["channel"] = map[string]any{
			"type":      "webhook",
			"webhookId": webhookID,
		}
	}

	return alertPair{
		Dashboard: map[string]any{
			"name":  "[Alert] " + alertName,
			"tiles": []any{tile},
			"tags":  dashboardTags,
		},
		Alert: alert,
	}
}

func extractMetricName(expr string) string {
	if matches := selectorPattern.FindAllStringSubmatch(expr, -1); len(matches) > 0 {
		for _, match := range matches {
			candidate := match[1]
			if !looksLikePromQLFunction(candidate) {
				return candidate
			}
		}
	}

	tokens := tokenPattern.FindAllString(expr, -1)
	for _, token := range tokens {
		if !looksLikePromQLFunction(token) {
			return token
		}
	}
	return "metric.value"
}

func extractOuterFn(expr string) string {
	match := outerPattern.FindStringSubmatch(expr)
	if len(match) < 2 {
		return ""
	}
	return strings.ToLower(match[1])
}

func extractLabelFilter(expr string) string {
	selectorStart := strings.Index(expr, "{")
	selectorEnd := strings.Index(expr, "}")
	if selectorStart == -1 || selectorEnd == -1 || selectorEnd <= selectorStart {
		return ""
	}

	selector := expr[selectorStart+1 : selectorEnd]
	matches := labelMatcherPattern.FindAllStringSubmatch(selector, -1)
	parts := make([]string, 0, len(matches))
	for _, match := range matches {
		label := match[1]
		op := match[2]
		value := escapeSQLString(match[3])
		sqlOperator := "="
		sqlValue := value
		switch op {
		case "=":
			sqlOperator = "="
		case "!=":
			sqlOperator = "!="
		case "=~":
			sqlOperator = "LIKE"
			sqlValue = strings.ReplaceAll(sqlValue, ".*", "%")
		case "!~":
			sqlOperator = "NOT LIKE"
			sqlValue = strings.ReplaceAll(sqlValue, ".*", "%")
		}
		parts = append(parts, fmt.Sprintf("%s %s '%s'", label, sqlOperator, sqlValue))
	}

	return strings.Join(parts, " AND ")
}

func extractGroupBy(expr string) []any {
	match := groupByPattern.FindStringSubmatch(expr)
	if len(match) < 2 {
		return []any{}
	}
	items := strings.Split(match[1], ",")
	result := make([]any, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func extractLegendGroupBy(legend string) []any {
	trimmed := strings.TrimSpace(legend)
	if trimmed == "" || !strings.Contains(trimmed, "{{") {
		return []any{}
	}
	trimmed = strings.ReplaceAll(trimmed, "{{", "")
	trimmed = strings.ReplaceAll(trimmed, "}}", "")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return []any{}
	}
	return []any{trimmed}
}

func extractQuantileLevel(expr string) (float64, bool) {
	match := quantilePattern.FindStringSubmatch(expr)
	if len(match) < 2 {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func extractThreshold(expr string) (*float64, string) {
	cleaned := strings.Join(strings.Fields(expr), " ")
	match := comparisonPattern.FindStringSubmatch(cleaned)
	if len(match) < 3 {
		return nil, "above"
	}
	value, err := strconv.ParseFloat(match[2], 64)
	if err != nil {
		return nil, "above"
	}
	switch match[1] {
	case ">", ">=", "==", "!=":
		return &value, "above"
	case "<", "<=":
		return &value, "below"
	default:
		return &value, "above"
	}
}

func parseDurationSeconds(duration string) int {
	total := 0
	for _, match := range durationPattern.FindAllStringSubmatch(strings.ToLower(duration), -1) {
		value, err := strconv.Atoi(match[1])
		if err != nil {
			continue
		}
		switch match[2] {
		case "s":
			total += value
		case "m":
			total += value * 60
		case "h":
			total += value * 3600
		case "d":
			total += value * 86400
		case "w":
			total += value * 7 * 86400
		}
	}
	if total == 0 {
		return 60
	}
	return total
}

func nearestHDXInterval(duration string) string {
	seconds := parseDurationSeconds(duration)
	for _, interval := range supportedIntervals {
		if seconds <= intervalSeconds[interval] {
			return interval
		}
	}
	return "1d"
}
