package peer

import "net/http"

// A redirected login page must never acknowledge a telemetry/status POST.
// Clock and discovery GET requests retain their own client behavior.
func mutationHTTPClient(base *http.Client) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}
