// Package experimentnet maintains an experiment-only overlay and its HTTP
// control gateway. P2P traffic stays directly between Peer containers.
package experimentnet

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

type GatewayConfig struct {
	Token         string            `json:"token"`
	ControllerURL string            `json:"controllerUrl"`
	Agents        map[string]string `json:"agents"`
	Subnet        string            `json:"subnet"`
}

func authorized(r *http.Request, token string) bool {
	return token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) == 1
}

func controlURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("control endpoint must be an HTTP base URL")
	}
	return u, nil
}

func GatewayHandler(config GatewayConfig, transport http.RoundTripper) (http.Handler, error) {
	controller, err := controlURL(config.ControllerURL)
	if err != nil || config.Token == "" {
		return nil, errors.New("gateway requires a Controller URL and token")
	}
	_, subnet, err := net.ParseCIDR(config.Subnet)
	if err != nil || subnet.IP.To4() == nil {
		return nil, errors.New("gateway requires an IPv4 Peer subnet")
	}
	agents := make(map[string]*url.URL, len(config.Agents))
	for id, raw := range config.Agents {
		u, err := controlURL(raw)
		if err != nil || !identifier.MatchString(id) {
			return nil, errors.New("invalid gateway Agent endpoint")
		}
		agents[id] = u
	}
	proxy := &httputil.ReverseProxy{Transport: transport, Rewrite: func(pr *httputil.ProxyRequest) {
		target := pr.In.Context().Value(gatewayTargetKey{}).(*url.URL)
		pr.SetURL(target)
		// The validated handler has already removed the routing prefix.
		pr.Out.URL.Path, pr.Out.URL.RawPath = pr.In.URL.Path, ""
	}, ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "experiment control gateway upstream unavailable", http.StatusBadGateway)
	}}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Peers synchronize clocks against this existing public health endpoint.
		clock := r.Method == http.MethodGet && r.URL.Path == "/controller/api/v1/health"
		if !clock && !authorized(r, config.Token) {
			http.Error(w, "valid bearer token required", http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/health" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 3)
		var target *url.URL
		var path string
		switch {
		case len(parts) >= 2 && parts[0] == "controller":
			target, path = controller, strings.TrimPrefix(r.URL.Path, "/controller")
			if path != "/api/v1/health" && path != "/api/v1/bootstrap" && path != "/api/v1/discovery" {
				target = nil
			}
		case len(parts) == 3 && parts[0] == "agents":
			target, path = agents[parts[1]], "/"+parts[2]
			if path != "/api/v1/telemetry" && !(strings.HasPrefix(path, "/api/v1/nodes/") && strings.HasSuffix(path, "/status")) {
				target = nil
			}
		case len(parts) == 3 && parts[0] == "peers":
			ip := net.ParseIP(parts[1])
			if ip != nil && ip.To4() != nil && subnet.Contains(ip) && !ip.IsLoopback() && !ip.IsUnspecified() {
				target = &url.URL{Scheme: "http", Host: net.JoinHostPort(ip.String(), "18000")}
				path = "/" + parts[2]
				if path != "/health" && path != "/publish" && path != "/mesh-freeze" && path != "/topology" && path != "/profile" {
					target = nil
				}
			}
		}
		if target == nil {
			http.NotFound(w, r)
			return
		}
		r = r.Clone(context.WithValue(r.Context(), gatewayTargetKey{}, target))
		r.URL.Path, r.URL.RawPath = path, ""
		proxy.ServeHTTP(w, r)
	}), nil
}

type gatewayTargetKey struct{}

func RunGateway(ctx context.Context, config GatewayConfig) error {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxIdleConns, transport.MaxIdleConnsPerHost = 256, 64
	transport.ResponseHeaderTimeout = 45 * time.Second
	defer transport.CloseIdleConnections()
	handler, err := GatewayHandler(config, transport)
	if err != nil {
		return err
	}
	return serve(ctx, ":18081", handler)
}

func serve(ctx context.Context, listen string, handler http.Handler) error {
	server := &http.Server{Addr: listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, WriteTimeout: 4 * time.Minute}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-done:
		}
	}()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
