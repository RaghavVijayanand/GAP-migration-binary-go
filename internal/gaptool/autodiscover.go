package gaptool

import (
	"fmt"
	"os"
)

// resolveIDWithCredentials is a convenience wrapper used by the CLI handlers.
// It skips auto-discovery entirely when id is already non-empty or when no
// HyperDX credentials are provided (preserving backwards compatibility with
// callers that always supply the ID explicitly). Otherwise it constructs a
// hyperDXClient and delegates to the supplied resolver.
func resolveIDWithCredentials(
	hyperDXURL, hyperDXAPIKey, id string,
	resolve func(*hyperDXClient, string) (string, error),
) (string, error) {
	if id != "" {
		return id, nil
	}
	if hyperDXURL == "" || hyperDXAPIKey == "" {
		// No credentials — return the empty id as-is; downstream code will
		// handle a missing ID (e.g. use an empty source for convert-only).
		return id, nil
	}
	client, err := newHyperDXClient(hyperDXURL, hyperDXAPIKey)
	if err != nil {
		return "", fmt.Errorf("auto-discovery: %w", err)
	}
	return resolve(client, id)
}

// resolveMetricSourceID returns id unchanged when it is non-empty.
// Otherwise it queries the HyperDX API for available sources and returns the
// first one it finds. A warning is printed to stderr when multiple sources
// exist so the caller knows an explicit ID would be safer. An error is
// returned when no sources exist because a metric source ID is required for
// dashboard and alert conversion.
func resolveMetricSourceID(client *hyperDXClient, id string) (string, error) {
	if id != "" {
		return id, nil
	}

	sources, err := client.listSources()
	if err != nil {
		return "", fmt.Errorf("auto-discovery of metric source ID failed: %w", err)
	}

	if len(sources) == 0 {
		return "", fmt.Errorf("auto-discovery found no metric sources in HyperDX; " +
			"create a source first or supply hyperdx_metric_source_id explicitly")
	}

	resolved := firstString(sources[0]["_id"], sources[0]["id"])
	if resolved == "" {
		return "", fmt.Errorf("auto-discovery: first HyperDX source has no id field; " +
			"supply hyperdx_metric_source_id explicitly")
	}

	name := firstString(sources[0]["name"])
	nameDisplay := ""
	if name != "" {
		nameDisplay = fmt.Sprintf(" (%s)", name)
	}

	if len(sources) > 1 {
		fmt.Fprintf(os.Stderr, "[gap] WARNING: %d metric sources found in HyperDX; "+
			"using %q%s — supply hyperdx_metric_source_id explicitly to choose a different one\n",
			len(sources), resolved, nameDisplay)
	} else {
		fmt.Fprintf(os.Stderr, "[gap] auto-discovered metric source ID: %q%s\n", resolved, nameDisplay)
	}

	return resolved, nil
}

// resolveWebhookID returns id unchanged when it is non-empty.
// Otherwise it queries the HyperDX API for configured webhooks and returns
// the first one. Unlike resolveMetricSourceID, a missing webhook is not
// treated as an error because webhook notifications are optional for alert
// migration (alerts can still be created without a notification channel).
func resolveWebhookID(client *hyperDXClient, id string) (string, error) {
	if id != "" {
		return id, nil
	}

	webhooks, err := client.listWebhooks()
	if err != nil {
		// Best-effort: log and continue without a webhook ID.
		fmt.Fprintf(os.Stderr, "[gap] WARNING: auto-discovery of webhook ID failed (%v); "+
			"alerts will be created without a notification channel\n", err)
		return "", nil
	}

	if len(webhooks) == 0 {
		fmt.Fprintln(os.Stderr, "[gap] no webhooks found in HyperDX; "+
			"alerts will be created without a notification channel")
		return "", nil
	}

	resolved := firstString(webhooks[0]["_id"], webhooks[0]["id"])
	if resolved == "" {
		fmt.Fprintln(os.Stderr, "[gap] WARNING: first HyperDX webhook has no id field; "+
			"alerts will be created without a notification channel")
		return "", nil
	}

	name := firstString(webhooks[0]["name"])
	nameDisplay := ""
	if name != "" {
		nameDisplay = fmt.Sprintf(" (%s)", name)
	}

	if len(webhooks) > 1 {
		fmt.Fprintf(os.Stderr, "[gap] WARNING: %d webhooks found in HyperDX; "+
			"using %q%s — supply webhook_id explicitly to choose a different one\n",
			len(webhooks), resolved, nameDisplay)
	} else {
		fmt.Fprintf(os.Stderr, "[gap] auto-discovered webhook ID: %q%s\n", resolved, nameDisplay)
	}

	return resolved, nil
}
