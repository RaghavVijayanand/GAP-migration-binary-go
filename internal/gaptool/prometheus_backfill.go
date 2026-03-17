package gaptool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPrometheusURL      = "http://localhost:9090"
	defaultClickHouseHost     = "localhost"
	defaultClickHousePort     = 8123
	defaultClickHouseDatabase = "default"
	defaultClickHouseUsername = "default"
	defaultLookbackDays       = 3
	defaultStepSeconds        = 60
	defaultBatchSize          = 10000
	defaultMetricFilter       = `^(?!scrape_|ALERTS).*`
	defaultServiceName        = "prometheus-migrated"
	maxPointsPerChunk         = 10000

	metricTypeCounter   = "counter"
	metricTypeGauge     = "gauge"
	metricTypeHistogram = "histogram"
	metricTypeSummary   = "summary"
	metricTypeUnknown   = "unknown"
	metricTypeUntyped   = "untyped"
	metricTypeInfo      = "info"
	metricTypeStateset  = "stateset"
)

var prometheusTypeToTable = map[string]string{
	metricTypeCounter:   "otel_metrics_sum",
	metricTypeGauge:     "otel_metrics_gauge",
	metricTypeHistogram: "otel_metrics_histogram",
	metricTypeSummary:   "otel_metrics_summary",
	metricTypeUnknown:   "otel_metrics_gauge",
	metricTypeUntyped:   "otel_metrics_gauge",
	metricTypeInfo:      "otel_metrics_gauge",
	metricTypeStateset:  "otel_metrics_gauge",
}

var prometheusSuffixes = []string{"_total", "_created", "_bucket", "_count", "_sum", "_info"}
var histogramSubMetricSuffixes = []string{"_bucket", "_count", "_sum"}
var defaultSkippedMetricPrefixes = []string{"scrape_", "ALERTS", "up"}

type metricInfo struct {
	PromName    string
	MetricType  string
	OTelName    string
	TargetTable string
	HelpText    string
	Unit        string
}

type migrationStats struct {
	MetricsDiscovered int            `json:"metrics_discovered"`
	MetricsSkipped    int            `json:"metrics_skipped"`
	MetricsMigrated   int            `json:"metrics_migrated"`
	MetricsFailed     int            `json:"metrics_failed"`
	RowsInserted      map[string]int `json:"rows_inserted"`
	TimeRange         [2]string      `json:"time_range"`
	Errors            []string       `json:"errors"`
}

type prometheusMetadataEntry struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

type prometheusMatrixResult struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

type clickHouseDescribeResponse struct {
	Data []struct {
		Name string `json:"name"`
	} `json:"data"`
}

type clickHouseWriter struct {
	host        string
	port        int
	database    string
	username    string
	password    string
	batchSize   int
	dryRun      bool
	client      *http.Client
	columnCache map[string][]string
}

var gaugeColumns = []string{
	"ResourceSchemaUrl", "ResourceAttributes", "ServiceName",
	"ScopeName", "ScopeVersion", "ScopeAttributes",
	"MetricName", "MetricDescription", "MetricUnit",
	"Attributes", "StartTimeUnix", "TimeUnix", "Value", "Flags",
}

var sumColumns = append(append([]string{}, gaugeColumns...), "AggTemp", "IsMonotonic")

var histogramColumns = []string{
	"ResourceSchemaUrl", "ResourceAttributes", "ServiceName",
	"ScopeName", "ScopeVersion", "ScopeAttributes",
	"MetricName", "MetricDescription", "MetricUnit",
	"Attributes", "StartTimeUnix", "TimeUnix",
	"Count", "Sum", "BucketCounts", "ExplicitBounds",
	"Flags", "Min", "Max",
}

var summaryColumns = []string{
	"ResourceSchemaUrl", "ResourceAttributes", "ServiceName",
	"ScopeName", "ScopeVersion", "ScopeAttributes",
	"MetricName", "MetricDescription", "MetricUnit",
	"Attributes", "StartTimeUnix", "TimeUnix",
	"Count", "Sum", "ValueAtQuantiles.Quantile", "ValueAtQuantiles.Value",
	"Flags",
}

func defaultBackfillPrometheusRequest() backfillPrometheusRequest {
	return backfillPrometheusRequest{
		PrometheusURL:      defaultPrometheusURL,
		ClickHouseHost:     defaultClickHouseHost,
		ClickHousePort:     defaultClickHousePort,
		ClickHouseDatabase: defaultClickHouseDatabase,
		ClickHouseUsername: defaultClickHouseUsername,
		LookbackDays:       defaultLookbackDays,
		StepSeconds:        defaultStepSeconds,
		BatchSize:          defaultBatchSize,
		MetricFilter:       defaultMetricFilter,
		ServiceName:        defaultServiceName,
	}
}

func normalizeBackfillPrometheusRequest(req *backfillPrometheusRequest) {
	defaults := defaultBackfillPrometheusRequest()
	req.PrometheusURL = strings.TrimRight(strings.TrimSpace(firstString(req.PrometheusURL, defaults.PrometheusURL)), "/")
	req.ClickHouseHost = strings.TrimSpace(firstString(req.ClickHouseHost, defaults.ClickHouseHost))
	req.ClickHouseDatabase = strings.TrimSpace(firstString(req.ClickHouseDatabase, defaults.ClickHouseDatabase))
	req.ClickHouseUsername = strings.TrimSpace(firstString(req.ClickHouseUsername, defaults.ClickHouseUsername))
	req.ClickHousePassword = strings.TrimSpace(req.ClickHousePassword)
	req.MetricFilter = strings.TrimSpace(firstString(req.MetricFilter, defaults.MetricFilter))
	req.ServiceName = strings.TrimSpace(firstString(req.ServiceName, defaults.ServiceName))

	if req.ClickHousePort <= 0 {
		req.ClickHousePort = defaults.ClickHousePort
	}
	if req.LookbackDays <= 0 {
		req.LookbackDays = defaults.LookbackDays
	}
	if req.StepSeconds <= 0 {
		req.StepSeconds = defaults.StepSeconds
	}
	if req.BatchSize <= 0 {
		req.BatchSize = defaults.BatchSize
	}
}

func migrateHistoricalData(req backfillPrometheusRequest) (migrationStats, error) {
	normalizeBackfillPrometheusRequest(&req)

	stats := migrationStats{
		RowsInserted: map[string]int{},
		Errors:       []string{},
	}

	endTime := time.Now().UTC()
	startTime := endTime.Add(-time.Duration(req.LookbackDays) * 24 * time.Hour)
	stats.TimeRange = [2]string{endTime.Add(-time.Duration(req.LookbackDays) * 24 * time.Hour).Format(time.RFC3339), endTime.Format(time.RFC3339)}

	filterPattern, err := compileMetricFilter(req.MetricFilter)
	if err != nil {
		stats.Errors = append(stats.Errors, fmt.Sprintf("invalid filter regex: %v", err))
		return stats, err
	}

	prom := newPrometheusHistoryClient(req.PrometheusURL)
	metrics, skipped, err := discoverMetrics(prom, filterPattern)
	stats.MetricsDiscovered = len(metrics)
	stats.MetricsSkipped = skipped
	if err != nil {
		stats.Errors = append(stats.Errors, err.Error())
		return stats, err
	}
	if len(metrics) == 0 {
		return stats, nil
	}

	writer := newClickHouseWriter(req)
	startUnix := float64(startTime.Unix())
	endUnix := float64(endTime.Unix())

	for _, info := range metrics {
		var (
			rows     []map[string]any
			inserted int
		)

		switch info.MetricType {
		case metricTypeHistogram:
			rows, err = extractHistogram(prom, info, startUnix, endUnix, req.StepSeconds, req.ServiceName)
			if err == nil {
				inserted, err = writer.insertHistogramRows(info.TargetTable, rows)
			}
		case metricTypeSummary:
			rows, err = extractSummary(prom, info, startUnix, endUnix, req.StepSeconds, req.ServiceName)
			if err == nil {
				inserted, err = writer.insertSummaryRows(info.TargetTable, rows)
			}
		case metricTypeCounter:
			rows, err = extractGaugeOrCounter(prom, info, startUnix, endUnix, req.StepSeconds, req.ServiceName)
			if err == nil {
				inserted, err = writer.insertSumRows(info.TargetTable, rows)
			}
		default:
			rows, err = extractGaugeOrCounter(prom, info, startUnix, endUnix, req.StepSeconds, req.ServiceName)
			if err == nil {
				inserted, err = writer.insertGaugeRows(info.TargetTable, rows)
			}
		}

		if err != nil {
			stats.MetricsFailed++
			stats.Errors = append(stats.Errors, fmt.Sprintf("%s: %v", info.PromName, err))
			continue
		}

		stats.RowsInserted[info.TargetTable] += inserted
		stats.MetricsMigrated++
	}

	return stats, nil
}

func compileMetricFilter(pattern string) (*regexp.Regexp, error) {
	trimmed := strings.TrimSpace(pattern)
	if trimmed == "" {
		return nil, nil
	}
	if trimmed == defaultMetricFilter {
		return nil, nil
	}
	return regexp.Compile(trimmed)
}

func promNameToOTel(name string) string {
	base := name
	for _, suffix := range prometheusSuffixes {
		if strings.HasSuffix(base, suffix) {
			base = strings.TrimSuffix(base, suffix)
			break
		}
	}
	return strings.ReplaceAll(base, "_", ".")
}

func isSkipMetric(name string, filterPattern *regexp.Regexp) bool {
	for _, prefix := range defaultSkippedMetricPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return filterPattern != nil && !filterPattern.MatchString(name)
}

type prometheusHistoryClient struct {
	baseURL string
	client  *http.Client
}

func newPrometheusHistoryClient(baseURL string) *prometheusHistoryClient {
	return &prometheusHistoryClient{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  &http.Client{Timeout: 30 * time.Second, Transport: newHTTPClient().Transport},
	}
}

func (c *prometheusHistoryClient) get(path string, params url.Values, target any) error {
	if params == nil {
		params = url.Values{}
	}
	requestURL := c.baseURL + path
	if encoded := params.Encode(); encoded != "" {
		requestURL += "?" + encoded
	}

	resp, err := c.client.Get(requestURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request to %s failed with status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var envelope struct {
		Status    string          `json:"status"`
		Data      json.RawMessage `json:"data"`
		ErrorType string          `json:"errorType"`
		Error     string          `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if envelope.Status != "success" {
		return fmt.Errorf("prometheus API error at %s: %s: %s", path, envelope.ErrorType, envelope.Error)
	}
	if target == nil {
		return nil
	}
	return json.Unmarshal(envelope.Data, target)
}

func (c *prometheusHistoryClient) getAllMetricNames() ([]string, error) {
	var names []string
	if err := c.get("/api/v1/label/__name__/values", nil, &names); err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (c *prometheusHistoryClient) getMetadata() (map[string][]prometheusMetadataEntry, error) {
	metadata := map[string][]prometheusMetadataEntry{}
	if err := c.get("/api/v1/metadata", nil, &metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (c *prometheusHistoryClient) getMetricType(metadata map[string][]prometheusMetadataEntry, name string) string {
	entries := metadata[name]
	if len(entries) > 0 && strings.TrimSpace(entries[0].Type) != "" {
		return entries[0].Type
	}
	if strings.HasSuffix(name, "_total") || strings.HasSuffix(name, "_created") {
		return metricTypeCounter
	}
	if strings.HasSuffix(name, "_bucket") {
		return metricTypeHistogram
	}
	if strings.HasSuffix(name, "_count") || strings.HasSuffix(name, "_sum") {
		base := name
		for _, suffix := range []string{"_count", "_sum"} {
			if strings.HasSuffix(name, suffix) {
				base = strings.TrimSuffix(name, suffix)
				break
			}
		}
		entries = metadata[base]
		if len(entries) > 0 && strings.TrimSpace(entries[0].Type) != "" {
			return entries[0].Type
		}
	}
	return metricTypeUnknown
}

func (c *prometheusHistoryClient) queryRange(query string, start, end float64, step int) ([]prometheusMatrixResult, error) {
	var payload struct {
		Result []prometheusMatrixResult `json:"result"`
	}
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatFloat(start, 'f', -1, 64))
	params.Set("end", strconv.FormatFloat(end, 'f', -1, 64))
	params.Set("step", strconv.Itoa(step))
	if err := c.get("/api/v1/query_range", params, &payload); err != nil {
		return nil, err
	}
	return payload.Result, nil
}

func (c *prometheusHistoryClient) queryRangeChunked(query string, start, end float64, step int) ([]prometheusMatrixResult, error) {
	totalSteps := int((end - start) / float64(step))
	if totalSteps <= maxPointsPerChunk {
		return c.queryRange(query, start, end, step)
	}

	chunkDuration := float64(maxPointsPerChunk * step)
	merged := map[string]prometheusMatrixResult{}
	for chunkStart := start; chunkStart < end; chunkStart += chunkDuration {
		chunkEnd := math.Min(chunkStart+chunkDuration, end)
		results, err := c.queryRange(query, chunkStart, chunkEnd, step)
		if err != nil {
			continue
		}
		for _, result := range results {
			key := seriesKey(result.Metric)
			existing := merged[key]
			if existing.Metric == nil {
				existing.Metric = cloneStringMap(result.Metric)
			}
			existing.Values = append(existing.Values, result.Values...)
			merged[key] = existing
		}
	}

	final := make([]prometheusMatrixResult, 0, len(merged))
	for _, result := range merged {
		seen := map[string]struct{}{}
		deduped := make([][]any, 0, len(result.Values))
		for _, sample := range result.Values {
			if len(sample) < 2 {
				continue
			}
			key := toString(sample[0])
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			deduped = append(deduped, sample)
		}
		sort.Slice(deduped, func(i, j int) bool {
			left, _ := floatFromAny(deduped[i][0])
			right, _ := floatFromAny(deduped[j][0])
			return left < right
		})
		result.Values = deduped
		final = append(final, result)
	}

	return final, nil
}

func discoverMetrics(prom *prometheusHistoryClient, filterPattern *regexp.Regexp) ([]metricInfo, int, error) {
	names, err := prom.getAllMetricNames()
	if err != nil {
		return nil, 0, err
	}
	metadata, err := prom.getMetadata()
	if err != nil {
		return nil, 0, err
	}

	histogramBases := map[string]struct{}{}
	summaryBases := map[string]struct{}{}
	baseSuffixes := map[string]map[string]struct{}{}
	for _, name := range names {
		for _, suffix := range histogramSubMetricSuffixes {
			if strings.HasSuffix(name, suffix) {
				base := strings.TrimSuffix(name, suffix)
				if baseSuffixes[base] == nil {
					baseSuffixes[base] = map[string]struct{}{}
				}
				baseSuffixes[base][suffix] = struct{}{}
				break
			}
		}
	}
	for base, suffixes := range baseSuffixes {
		entries := metadata[base]
		if len(entries) > 0 {
			switch entries[0].Type {
			case metricTypeHistogram:
				histogramBases[base] = struct{}{}
				continue
			case metricTypeSummary:
				summaryBases[base] = struct{}{}
				continue
			}
		}
		if _, ok := suffixes["_bucket"]; ok {
			histogramBases[base] = struct{}{}
		} else if len(suffixes) >= 2 {
			summaryBases[base] = struct{}{}
		}
	}

	nameSet := map[string]struct{}{}
	for _, name := range names {
		nameSet[name] = struct{}{}
	}

	metrics := make([]metricInfo, 0, len(names))
	seenGroupedBases := map[string]struct{}{}
	skipped := 0

	for _, name := range names {
		if isSkipMetric(name, filterPattern) {
			skipped++
			continue
		}

		grouped := false
		for _, suffix := range histogramSubMetricSuffixes {
			if !strings.HasSuffix(name, suffix) {
				continue
			}
			base := strings.TrimSuffix(name, suffix)
			if _, ok := histogramBases[base]; ok {
				if _, seen := seenGroupedBases[base]; !seen {
					seenGroupedBases[base] = struct{}{}
					metrics = append(metrics, buildMetricInfo(metadata, base, metricTypeHistogram))
				}
				grouped = true
				break
			}
			if _, ok := summaryBases[base]; ok {
				if _, seen := seenGroupedBases[base]; !seen {
					seenGroupedBases[base] = struct{}{}
					metrics = append(metrics, buildMetricInfo(metadata, base, metricTypeSummary))
				}
				grouped = true
				break
			}
		}
		if grouped {
			continue
		}

		metricType := prom.getMetricType(metadata, name)
		if metricType == metricTypeUnknown {
			switch {
			case strings.HasSuffix(name, "_info"):
				metricType = metricTypeGauge
			case strings.HasSuffix(name, "_ratio"), strings.HasSuffix(name, "_percent"):
				metricType = metricTypeGauge
			case strings.HasSuffix(name, "_bytes"), strings.HasSuffix(name, "_seconds"):
				if _, ok := nameSet[name+"_total"]; ok {
					metricType = metricTypeCounter
				} else {
					metricType = metricTypeGauge
				}
			}
		}

		if metricType == metricTypeHistogram || metricType == metricTypeSummary {
			seenGroupedBases[name] = struct{}{}
		}

		metrics = append(metrics, buildMetricInfo(metadata, name, metricType))
	}

	return metrics, skipped, nil
}

func buildMetricInfo(metadata map[string][]prometheusMetadataEntry, name, metricType string) metricInfo {
	return metricInfo{
		PromName:    name,
		MetricType:  metricType,
		OTelName:    promNameToOTel(name),
		TargetTable: prometheusTypeToTable[metricType],
		HelpText:    metadataField(metadata, name, func(entry prometheusMetadataEntry) string { return entry.Help }),
		Unit:        metadataField(metadata, name, func(entry prometheusMetadataEntry) string { return entry.Unit }),
	}
}

func metadataField(metadata map[string][]prometheusMetadataEntry, name string, getter func(prometheusMetadataEntry) string) string {
	entries := metadata[name]
	if len(entries) == 0 {
		return ""
	}
	return getter(entries[0])
}

func extractGaugeOrCounter(prom *prometheusHistoryClient, info metricInfo, start, end float64, step int, serviceName string) ([]map[string]any, error) {
	results, err := prom.queryRangeChunked(info.PromName, start, end, step)
	if err != nil {
		return nil, err
	}

	rows := []map[string]any{}
	resourceAttributes := makeResourceAttributes(serviceName)
	for _, series := range results {
		attributes := makeAttributes(series.Metric)
		for _, sample := range series.Values {
			ts, value, ok := parseSample(sample)
			if !ok {
				continue
			}
			if math.IsNaN(value) {
				value = 0
			}
			if math.IsInf(value, 0) {
				continue
			}
			row := baseMetricRow(info, resourceAttributes, serviceName, attributes, ts)
			row["Value"] = value
			if info.MetricType == metricTypeCounter {
				row["AggTemp"] = 2
				row["IsMonotonic"] = true
			}
			rows = append(rows, row)
		}
	}

	return rows, nil
}

func extractHistogram(prom *prometheusHistoryClient, info metricInfo, start, end float64, step int, serviceName string) ([]map[string]any, error) {
	bucketResults, err := prom.queryRangeChunked(info.PromName+"_bucket", start, end, step)
	if err != nil {
		return nil, err
	}
	countResults, err := prom.queryRangeChunked(info.PromName+"_count", start, end, step)
	if err != nil {
		return nil, err
	}
	sumResults, err := prom.queryRangeChunked(info.PromName+"_sum", start, end, step)
	if err != nil {
		return nil, err
	}

	countLookup := sampleLookup(countResults)
	sumLookup := sampleLookup(sumResults)
	bucketData := map[string]map[float64]float64{}
	labelSets := map[string]map[string]string{}

	for _, series := range bucketResults {
		labels := cloneStringMap(series.Metric)
		delete(labels, "__name__")
		le := labels["le"]
		delete(labels, "le")
		if strings.TrimSpace(le) == "" {
			continue
		}
		bound, err := parsePrometheusBound(le)
		if err != nil {
			continue
		}
		seriesKeyNoLE := seriesKey(labels)
		labelSets[seriesKeyNoLE] = cloneStringMap(labels)
		for _, sample := range series.Values {
			ts, value, ok := parseSample(sample)
			if !ok {
				continue
			}
			entryKey := timestampedSeriesKey(seriesKeyNoLE, ts)
			if bucketData[entryKey] == nil {
				bucketData[entryKey] = map[float64]float64{}
			}
			bucketData[entryKey][bound] = value
		}
	}

	rows := []map[string]any{}
	resourceAttributes := makeResourceAttributes(serviceName)
	for entryKey, bounds := range bucketData {
		seriesKeyNoLE, ts := splitTimestampedSeriesKey(entryKey)
		finiteBounds := make([]float64, 0, len(bounds))
		for bound := range bounds {
			if math.IsInf(bound, 0) {
				continue
			}
			finiteBounds = append(finiteBounds, bound)
		}
		sort.Float64s(finiteBounds)

		cumulative := make([]float64, 0, len(finiteBounds)+1)
		for _, bound := range finiteBounds {
			cumulative = append(cumulative, bounds[bound])
		}
		if infCount, ok := bounds[math.Inf(1)]; ok {
			cumulative = append(cumulative, infCount)
		} else if len(cumulative) > 0 {
			cumulative = append(cumulative, cumulative[len(cumulative)-1])
		} else {
			cumulative = append(cumulative, 0)
		}

		bucketCounts := make([]int, 0, len(cumulative))
		previous := 0.0
		for _, count := range cumulative {
			delta := count - previous
			if delta < 0 {
				delta = 0
			}
			bucketCounts = append(bucketCounts, int(delta))
			previous = count
		}

		attributes := makeAttributes(labelSets[seriesKeyNoLE])
		row := baseMetricRow(info, resourceAttributes, serviceName, attributes, ts)
		row["Count"] = int(countLookup[entryKey])
		row["Sum"] = sanitizeFiniteFloat(sumLookup[entryKey])
		row["BucketCounts"] = bucketCounts
		row["ExplicitBounds"] = finiteBounds
		row["Min"] = 0.0
		row["Max"] = 0.0
		rows = append(rows, row)
	}

	return rows, nil
}

func extractSummary(prom *prometheusHistoryClient, info metricInfo, start, end float64, step int, serviceName string) ([]map[string]any, error) {
	quantileResults, err := prom.queryRangeChunked(info.PromName, start, end, step)
	if err != nil {
		return nil, err
	}
	countResults, err := prom.queryRangeChunked(info.PromName+"_count", start, end, step)
	if err != nil {
		return nil, err
	}
	sumResults, err := prom.queryRangeChunked(info.PromName+"_sum", start, end, step)
	if err != nil {
		return nil, err
	}

	countLookup := sampleLookup(countResults)
	sumLookup := sampleLookup(sumResults)
	quantileData := map[string]map[float64]float64{}
	labelSets := map[string]map[string]string{}

	for _, series := range quantileResults {
		labels := cloneStringMap(series.Metric)
		delete(labels, "__name__")
		quantile := labels["quantile"]
		delete(labels, "quantile")
		if strings.TrimSpace(quantile) == "" {
			continue
		}
		quantileValue, ok := floatFromAny(quantile)
		if !ok {
			continue
		}
		seriesKeyNoQuantile := seriesKey(labels)
		labelSets[seriesKeyNoQuantile] = cloneStringMap(labels)
		for _, sample := range series.Values {
			ts, value, ok := parseSample(sample)
			if !ok {
				continue
			}
			entryKey := timestampedSeriesKey(seriesKeyNoQuantile, ts)
			if quantileData[entryKey] == nil {
				quantileData[entryKey] = map[float64]float64{}
			}
			quantileData[entryKey][quantileValue] = sanitizeFiniteFloat(value)
		}
	}

	rows := []map[string]any{}
	resourceAttributes := makeResourceAttributes(serviceName)
	for entryKey, quantiles := range quantileData {
		seriesKeyNoQuantile, ts := splitTimestampedSeriesKey(entryKey)
		sortedQuantiles := make([]float64, 0, len(quantiles))
		for quantile := range quantiles {
			sortedQuantiles = append(sortedQuantiles, quantile)
		}
		sort.Float64s(sortedQuantiles)
		values := make([]float64, 0, len(sortedQuantiles))
		for _, quantile := range sortedQuantiles {
			values = append(values, quantiles[quantile])
		}

		attributes := makeAttributes(labelSets[seriesKeyNoQuantile])
		row := baseMetricRow(info, resourceAttributes, serviceName, attributes, ts)
		row["Count"] = int(countLookup[entryKey])
		row["Sum"] = sanitizeFiniteFloat(sumLookup[entryKey])
		row["ValueAtQuantiles.Quantile"] = sortedQuantiles
		row["ValueAtQuantiles.Value"] = values
		rows = append(rows, row)
	}

	return rows, nil
}

func sampleLookup(results []prometheusMatrixResult) map[string]float64 {
	lookup := map[string]float64{}
	for _, series := range results {
		labels := cloneStringMap(series.Metric)
		delete(labels, "__name__")
		key := seriesKey(labels)
		for _, sample := range series.Values {
			ts, value, ok := parseSample(sample)
			if !ok {
				continue
			}
			lookup[timestampedSeriesKey(key, ts)] = sanitizeFiniteFloat(value)
		}
	}
	return lookup
}

func parsePrometheusBound(value string) (float64, error) {
	if value == "+Inf" {
		return math.Inf(1), nil
	}
	return strconv.ParseFloat(value, 64)
}

func parseSample(sample []any) (float64, float64, bool) {
	if len(sample) < 2 {
		return 0, 0, false
	}
	ts, ok := floatFromAny(sample[0])
	if !ok {
		return 0, 0, false
	}
	value, ok := floatFromAny(sample[1])
	if !ok {
		return 0, 0, false
	}
	return ts, value, true
}

func sanitizeFiniteFloat(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func makeAttributes(labels map[string]string) map[string]string {
	attributes := map[string]string{}
	for key, value := range labels {
		if key == "__name__" || key == "le" || key == "quantile" {
			continue
		}
		attributes[key] = value
	}
	return attributes
}

func makeResourceAttributes(serviceName string) map[string]string {
	return map[string]string{
		"service.name":       serviceName,
		"telemetry.sdk.name": "prometheus-migrator",
	}
}

func baseMetricRow(info metricInfo, resourceAttributes map[string]string, serviceName string, attributes map[string]string, ts float64) map[string]any {
	timestamp := clickHouseTimeString(ts)
	return map[string]any{
		"ResourceSchemaUrl":  "",
		"ResourceAttributes": resourceAttributes,
		"ServiceName":        serviceName,
		"ScopeName":          "prometheus-migrator",
		"ScopeVersion":       "1.0.0",
		"ScopeAttributes":    map[string]string{},
		"MetricName":         info.OTelName,
		"MetricDescription":  info.HelpText,
		"MetricUnit":         info.Unit,
		"Attributes":         attributes,
		"StartTimeUnix":      timestamp,
		"TimeUnix":           timestamp,
		"Flags":              0,
	}
}

func clickHouseTimeString(ts float64) string {
	seconds := int64(ts)
	nanos := int64((ts - float64(seconds)) * float64(time.Second))
	return time.Unix(seconds, nanos).UTC().Format("2006-01-02 15:04:05")
}

func seriesKey(metric map[string]string) string {
	if len(metric) == 0 {
		return ""
	}
	keys := make([]string, 0, len(metric))
	for key := range metric {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+metric[key])
	}
	return strings.Join(parts, "|")
}

func timestampedSeriesKey(seriesKey string, ts float64) string {
	return seriesKey + "@@" + strconv.FormatFloat(ts, 'f', -1, 64)
}

func splitTimestampedSeriesKey(value string) (string, float64) {
	parts := strings.SplitN(value, "@@", 2)
	if len(parts) != 2 {
		return value, 0
	}
	ts, _ := strconv.ParseFloat(parts[1], 64)
	return parts[0], ts
}

func cloneStringMap(source map[string]string) map[string]string {
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func newClickHouseWriter(req backfillPrometheusRequest) *clickHouseWriter {
	return &clickHouseWriter{
		host:        req.ClickHouseHost,
		port:        req.ClickHousePort,
		database:    req.ClickHouseDatabase,
		username:    req.ClickHouseUsername,
		password:    req.ClickHousePassword,
		batchSize:   req.BatchSize,
		dryRun:      req.DryRun,
		client:      &http.Client{Timeout: 30 * time.Second, Transport: newHTTPClient().Transport},
		columnCache: map[string][]string{},
	}
}

func (w *clickHouseWriter) insertGaugeRows(table string, rows []map[string]any) (int, error) {
	return w.insertRows(table, rows, gaugeColumns)
}

func (w *clickHouseWriter) insertSumRows(table string, rows []map[string]any) (int, error) {
	return w.insertRows(table, rows, sumColumns)
}

func (w *clickHouseWriter) insertHistogramRows(table string, rows []map[string]any) (int, error) {
	return w.insertRows(table, rows, histogramColumns)
}

func (w *clickHouseWriter) insertSummaryRows(table string, rows []map[string]any) (int, error) {
	return w.insertRows(table, rows, summaryColumns)
}

func (w *clickHouseWriter) insertRows(table string, rows []map[string]any, desiredColumns []string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if w.dryRun {
		return len(rows), nil
	}

	columns, err := w.detectColumns(table, desiredColumns)
	if err != nil {
		return 0, err
	}
	if len(columns) == 0 {
		columns = desiredColumns
	}

	total := 0
	for start := 0; start < len(rows); start += w.batchSize {
		end := start + w.batchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := w.insertBatch(table, columns, rows[start:end]); err != nil {
			return total, err
		}
		total += end - start
	}

	return total, nil
}

func (w *clickHouseWriter) detectColumns(table string, desiredColumns []string) ([]string, error) {
	cacheKey := w.database + "." + table
	if cached, ok := w.columnCache[cacheKey]; ok {
		return cached, nil
	}

	query := fmt.Sprintf("DESCRIBE TABLE %s.%s FORMAT JSON", quoteIdentifier(w.database), quoteIdentifier(table))
	body, err := w.doQuery(query, nil)
	if err != nil {
		return desiredColumns, nil
	}

	var response clickHouseDescribeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return desiredColumns, nil
	}
	actual := map[string]struct{}{}
	for _, row := range response.Data {
		actual[row.Name] = struct{}{}
	}
	columns := make([]string, 0, len(desiredColumns))
	for _, column := range desiredColumns {
		if _, ok := actual[column]; ok {
			columns = append(columns, column)
		}
	}
	w.columnCache[cacheKey] = columns
	return columns, nil
}

func (w *clickHouseWriter) insertBatch(table string, columns []string, rows []map[string]any) error {
	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) FORMAT JSONEachRow",
		quoteIdentifier(w.database),
		quoteIdentifier(table),
		joinQuotedIdentifiers(columns),
	)

	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, row := range rows {
		filtered := map[string]any{}
		for _, column := range columns {
			if value, ok := row[column]; ok {
				filtered[column] = value
			}
		}
		if err := encoder.Encode(filtered); err != nil {
			return err
		}
	}

	_, err := w.doQuery(query, &body)
	return err
}

func (w *clickHouseWriter) doQuery(query string, body io.Reader) ([]byte, error) {
	endpoint := fmt.Sprintf("http://%s:%d/", w.host, w.port)
	params := url.Values{}
	params.Set("query", query)
	requestURL := endpoint + "?" + params.Encode()

	request, err := http.NewRequest(http.MethodPost, requestURL, body)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(w.username) != "" {
		request.SetBasicAuth(w.username, w.password)
	}
	if body != nil {
		request.Header.Set("Content-Type", "text/plain")
	}

	response, err := w.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("clickhouse request failed with status %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return responseBody, nil
}

func quoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func joinQuotedIdentifiers(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, quoteIdentifier(column))
	}
	return strings.Join(quoted, ", ")
}
