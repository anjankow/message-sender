package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

type HTTPClient struct {
	url string
}

func NewHTTPClient(url string) *HTTPClient {
	return &HTTPClient{
		url: url,
	}
}

func (h HTTPClient) Post(ctx context.Context, body bytes.Buffer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, &body)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to post a message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Read the possible reason for failure from the response
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}
