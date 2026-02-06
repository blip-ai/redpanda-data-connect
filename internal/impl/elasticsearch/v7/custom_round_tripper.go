package elasticsearch

import (
	"bytes"
	"io"
	"net/http"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// LoggerRoundTripper is an HTTP round tripper that logs requests and responses.
type LoggerRoundTripper struct {
	Transport http.RoundTripper
	Logger    *service.Logger
	Service   string
}

func (c *LoggerRoundTripper) transport() http.RoundTripper {
	if c.Transport != nil {
		return c.Transport
	}
	return http.DefaultTransport
}

// RoundTrip implements the http.RoundTripper interface.
func (c *LoggerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var requestBody string
	if req.Body != nil {
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			c.Logger.With("error", err).Warn("Failed to read request body")
			return nil, err
		}
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		requestBody = string(bodyBytes)
	}

	resp, err := c.transport().RoundTrip(req)
	if err != nil {
		return nil, err
	}

	var responseBody string
	var status int
	if resp.Body != nil {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			c.Logger.With("error", err).Warn("Failed to read response body")
			return nil, err
		}
		resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		status = resp.StatusCode
		responseBody = string(bodyBytes)
	}

	c.Logger.With("request_url", req.URL, "response_status", status, "request_body", requestBody, "response_body", responseBody, "requesting", c.Service).Info("Intercepting HTTP request")

	return resp, nil
}
