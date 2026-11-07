package agent

import "net/http"

// Internal mutations must be acknowledged by their addressed endpoint. A
// redirect to a successful login page is not delivery of telemetry or control.
func mutationHTTPClient(base *http.Client) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}
