package gaptool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type hyperDXClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

func newHyperDXClient(baseURL, apiKey string) (*hyperDXClient, error) {
	trimmedURL := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	trimmedAPIKey := strings.TrimSpace(apiKey)
	if trimmedURL == "" {
		return nil, fmt.Errorf("hyperdx URL is required")
	}
	if trimmedAPIKey == "" {
		return nil, fmt.Errorf("hyperdx API key is required")
	}
	return &hyperDXClient{
		baseURL: trimmedURL + "/api",
		apiKey:  trimmedAPIKey,
		client:  newHTTPClient(),
	}, nil
}

func (c *hyperDXClient) requestJSON(method, path string, payload any) (any, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(encoded)
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	req, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", hyperDXAuthorizationHeader(c.apiKey))
	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	var resp *http.Response
	maxRetries := 3
	backoffMs := 1000

	for attempt := 0; attempt <= maxRetries; attempt++ {
		resp, err = c.client.Do(req)
		if err != nil {
			return nil, err
		}

		// Success or unrecoverable error
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
			break
		}

		// Rate limited or temporarily unavailable — close body and retry
		resp.Body.Close()

		if attempt == maxRetries {
			return nil, fmt.Errorf("request to %s failed with status %d after %d retries", path, resp.StatusCode, maxRetries)
		}

		time.Sleep(time.Duration(backoffMs) * time.Millisecond)
		backoffMs *= 2

		// Need to reset the body reader if we have a payload
		if payload != nil {
			var marshalErr error
			encoded, marshalErr := json.Marshal(payload)
			if marshalErr != nil {
				return nil, marshalErr
			}
			req.Body = io.NopCloser(bytes.NewReader(encoded))
		}
	}
	defer resp.Body.Close()

	responseBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.TrimSpace(string(responseBody))
		if message == "" {
			message = resp.Status
		}
		return nil, fmt.Errorf("request to %s failed with status %d: %s", path, resp.StatusCode, message)
	}

	if len(bytes.TrimSpace(responseBody)) == 0 {
		return map[string]any{}, nil
	}

	var decoded any
	if unmarshalErr := json.Unmarshal(responseBody, &decoded); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return decoded, nil
}

func (c *hyperDXClient) listDashboards() ([]map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/v2/dashboards", nil)
	if err != nil {
		return nil, err
	}
	return listItems(decoded), nil
}

func (c *hyperDXClient) getDashboard(id string) (map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/v2/dashboards/"+id, nil)
	if err != nil {
		return nil, err
	}
	return asMap(decoded), nil
}

func (c *hyperDXClient) createDashboard(payload map[string]any) (map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodPost, "/v2/dashboards", payload)
	if err != nil {
		return nil, err
	}
	return asMap(decoded), nil
}

func (c *hyperDXClient) listAlerts() ([]map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/v2/alerts", nil)
	if err != nil {
		return nil, err
	}
	return listItems(decoded), nil
}

func (c *hyperDXClient) createAlert(payload map[string]any) (map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodPost, "/v2/alerts", payload)
	if err != nil {
		return nil, err
	}
	return asMap(decoded), nil
}

func (c *hyperDXClient) listSources() ([]map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/sources", nil)
	if err != nil {
		return nil, err
	}
	return listItems(decoded), nil
}

func (c *hyperDXClient) listWebhooks() ([]map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/webhooks", nil)
	if err != nil {
		return nil, err
	}
	return listItems(decoded), nil
}

func hyperDXAuthorizationHeader(apiKey string) string {
	trimmed := strings.TrimSpace(apiKey)
	if strings.HasPrefix(strings.ToLower(trimmed), "bearer ") {
		return trimmed
	}
	return "Bearer " + trimmed
}
