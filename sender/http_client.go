package sender

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

type HTTPError struct {
	Status int
	Body   string
}

func (e HTTPError) Error() string {
	return fmt.Sprintf("response error: status: %d, body: %s", e.Status, e.Body)
}

func (e HTTPError) Is(target error) bool {
	t, ok := target.(HTTPError)
	return ok && e.Status == t.Status
}

func ErrInvalidStatus(code int) error {
	return fmt.Errorf("invalid status code: %d", code)
}

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
		return HTTPError{
			Status: resp.StatusCode,
			Body:   string(bodyBytes),
		}
	}

	return nil
}
