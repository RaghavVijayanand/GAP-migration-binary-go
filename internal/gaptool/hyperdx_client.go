package gaptool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
		baseURL: trimmedURL + "/api/api/v2",
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
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
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
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func (c *hyperDXClient) listDashboards() ([]map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/dashboards", nil)
	if err != nil {
		return nil, err
	}
	return listItems(decoded), nil
}

func (c *hyperDXClient) getDashboard(id string) (map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/dashboards/"+id, nil)
	if err != nil {
		return nil, err
	}
	return asMap(decoded), nil
}

func (c *hyperDXClient) createDashboard(payload map[string]any) (map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodPost, "/dashboards", payload)
	if err != nil {
		return nil, err
	}
	return asMap(decoded), nil
}

func (c *hyperDXClient) listAlerts() ([]map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodGet, "/alerts", nil)
	if err != nil {
		return nil, err
	}
	return listItems(decoded), nil
}

func (c *hyperDXClient) createAlert(payload map[string]any) (map[string]any, error) {
	decoded, err := c.requestJSON(http.MethodPost, "/alerts", payload)
	if err != nil {
		return nil, err
	}
	return asMap(decoded), nil
}
