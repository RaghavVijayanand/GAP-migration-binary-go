package gaptool

type convertGrafanaRequest struct {
	GapData               map[string]any `json:"gap_data"`
	HyperDXMetricSourceID string         `json:"hyperdx_metric_source_id"` // optional — auto-discovered when empty
	// Credentials are only needed when hyperdx_metric_source_id is omitted; used for auto-discovery.
	HyperDXURL    string `json:"hyperdx_url"`
	HyperDXAPIKey string `json:"hyperdx_api_key"`
}

type applyGrafanaRequest struct {
	HyperDXURL    string           `json:"hyperdx_url"`
	HyperDXAPIKey string           `json:"hyperdx_api_key"`
	Dashboards    []map[string]any `json:"dashboards"`
	DryRun        bool             `json:"dry_run"`
}

type convertAlertsRequest struct {
	GapData               map[string]any `json:"gap_data"`
	HyperDXMetricSourceID string         `json:"hyperdx_metric_source_id"` // optional — auto-discovered when empty
	WebhookID             string         `json:"webhook_id"`               // optional — auto-discovered when empty
	// Credentials are only needed when IDs are omitted; used for auto-discovery.
	HyperDXURL    string `json:"hyperdx_url"`
	HyperDXAPIKey string `json:"hyperdx_api_key"`
}


type applyAlertsRequest struct {
	HyperDXURL    string      `json:"hyperdx_url"`
	HyperDXAPIKey string      `json:"hyperdx_api_key"`
	AlertPairs    []alertPair `json:"alert_pairs"`
	DryRun        bool        `json:"dry_run"`
}

type backfillPrometheusRequest struct {
	PrometheusURL      string `json:"prometheus_url"`
	ClickHouseHost     string `json:"clickhouse_host"`
	ClickHousePort     int    `json:"clickhouse_port"`
	ClickHouseDatabase string `json:"clickhouse_database"`
	ClickHouseUsername string `json:"clickhouse_username"`
	ClickHousePassword string `json:"clickhouse_password"`
	LookbackDays       int    `json:"lookback_days"`
	StepSeconds        int    `json:"step_seconds"`
	BatchSize          int    `json:"batch_size"`
	MetricFilter       string `json:"metric_filter"`
	ServiceName        string `json:"service_name"`
	DryRun             bool   `json:"dry_run"`
}
