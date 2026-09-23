package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/v3/internal/auth"
	"github.com/k-p2p-lab/v3/internal/webui"
)

const sessionCookieName = "kpl_session"
const sessionLifetime = 12 * time.Hour
const sessionLimit = 1024

type browserSession struct {
	expires time.Time
	done    chan struct{}
}
type loginAttempts struct {
	count   int
	expires time.Time
}
type browserAuth struct {
	mu       sync.Mutex
	sessions map[[32]byte]*browserSession
	failures map[string]loginAttempts
}

func newBrowserAuth() *browserAuth {
	return &browserAuth{sessions: make(map[[32]byte]*browserSession), failures: make(map[string]loginAttempts)}
}

func (a *browserAuth) session(r *http.Request) *browserSession {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || len(cookie.Value) != 64 {
		return nil
	}
	key := sha256.Sum256([]byte(cookie.Value))
	a.mu.Lock()
	defer a.mu.Unlock()
	session := a.sessions[key]
	if session != nil && !time.Now().Before(session.expires) {
		close(session.done)
		delete(a.sessions, key)
		return nil
	}
	return session
}

func (a *browserAuth) revoke(r *http.Request) {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return
	}
	key := sha256.Sum256([]byte(cookie.Value))
	a.mu.Lock()
	defer a.mu.Unlock()
	if session := a.sessions[key]; session != nil {
		close(session.done)
		delete(a.sessions, key)
	}
}

func secureRequest(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func sameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	if raw := r.Header.Get("Origin"); raw != "" {
		origin, err := url.Parse(raw)
		scheme := "http"
		if secureRequest(r) {
			scheme = "https"
		}
		return err == nil && origin.Scheme == scheme && strings.EqualFold(origin.Host, r.Host) && origin.User == nil && origin.Path == "" && origin.RawQuery == "" && origin.Fragment == ""
	}
	return true
}

func publicAuthPath(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	switch r.URL.Path {
	case "/login", "/login.html", "/login-required", "/login-required.html", "/login.js", "/login.css", "/styles.css", "/kpl-logo.jpg", "/favicon.ico",
		"/api/v1/health", "/metrics", "/api/v1/prometheus/agent-targets", "/api/v1/prometheus/controller-targets":
		return true
	}
	return false
}

func internalAuthPath(r *http.Request) bool {
	if r.Method == http.MethodGet {
		return r.URL.Path == "/api/v1/bootstrap" || r.URL.Path == "/api/v1/discovery"
	}
	if r.Method == http.MethodPost {
		switch r.URL.Path {
		case "/api/v1/agents/register", "/api/v1/agents/heartbeat", "/api/v1/events", "/api/v1/events/batch":
			return true
		}
	}
	return false
}

func (s *Server) withAuthentication(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publicAuthPath(r) || (r.URL.Path == "/api/v1/auth/login" && r.Method == http.MethodPost) {
			next.ServeHTTP(w, r)
			return
		}
		if internalAuthPath(r) && s.config.Token != "" && auth.Equal(r.Header.Get("Authorization"), "Bearer "+s.config.Token) {
			setWebIdentity(r, "internal", "")
			next.ServeHTTP(w, r)
			return
		}
		session := s.auth.session(r)
		if session == nil {
			recordWebAuth(r, "", "failure", "login_required", "")
			w.Header().Set("Cache-Control", "no-store")
			if (r.URL.Path == "/" || r.URL.Path == "/index.html") && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
			} else if wantsLoginRequiredHTML(r) {
				serveLoginRequired(w, r)
			} else {
				writeError(w, http.StatusUnauthorized, "login required")
			}
			return
		}
		setWebIdentity(r, "session", s.config.User)
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet && r.Method != http.MethodHead && (!sameOrigin(r) || r.Header.Get("X-KPL-Request") != "dashboard") {
			recordWebAuth(r, "", "failure", "origin_denied", s.config.User)
			writeError(w, http.StatusForbidden, "same-origin request required")
			return
		}
		if r.URL.Path == "/api/v1/stream" {
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			timer := time.NewTimer(time.Until(session.expires))
			defer timer.Stop()
			go func() {
				select {
				case <-session.done:
					cancel()
				case <-timer.C:
					cancel()
				case <-ctx.Done():
				}
			}()
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// HTML is for browser navigation; API/SSE clients retain the JSON 401 contract.
func wantsLoginRequiredHTML(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	if r.Header.Get("Sec-Fetch-Mode") == "navigate" {
		return true
	}
	var htmlQuality, jsonQuality float64
	for _, item := range strings.Split(r.Header.Get("Accept"), ",") {
		media, params, err := mime.ParseMediaType(strings.TrimSpace(item))
		if err != nil {
			continue
		}
		quality := 1.0
		if raw, exists := params["q"]; exists {
			quality, err = strconv.ParseFloat(raw, 64)
			if err != nil || !(quality >= 0 && quality <= 1) {
				continue
			}
		}
		switch media {
		case "text/html":
			htmlQuality = max(htmlQuality, quality)
		case "application/json":
			jsonQuality = max(jsonQuality, quality)
		}
	}
	return htmlQuality > 0 && htmlQuality >= jsonQuality
}

func serveLoginRequired(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w)
		return
	}
	page, err := fs.ReadFile(webui.FS(), "login-required.html")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load login page")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusUnauthorized)
	if r.Method != http.MethodHead {
		_, _ = w.Write(page)
	}
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	recordWebAuth(r, "login", "failure", "invalid_request", "")
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !sameOrigin(r) {
		recordWebAuth(r, "login", "failure", "origin_denied", "")
		writeError(w, http.StatusForbidden, "same-origin request required")
		return
	}
	if err := auth.Validate(s.config.User, s.config.Password); err != nil {
		recordWebAuth(r, "login", "failure", "not_configured", "")
		writeError(w, http.StatusServiceUnavailable, "login is not configured")
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		recordWebAuth(r, "login", "failure", "unsupported_media_type", "")
		writeError(w, http.StatusUnsupportedMediaType, "application/json required")
		return
	}
	var credentials struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&credentials); err != nil {
		writeError(w, http.StatusBadRequest, "invalid login request")
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid login request")
		return
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	var previousSession string
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		previousSession = cookie.Value
	}
	result := s.authenticateLogin(host, credentials.User, credentials.Password, previousSession)
	// Network writes must never hold the shared session lock: a slow login
	// client must not block authentication, logout, or SSE session revocation.
	if result.status != http.StatusOK {
		if result.status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "300")
		}
		recordWebAuth(r, "login", "failure", result.reason, credentials.User)
		writeError(w, result.status, result.message)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: result.value, Path: "/", HttpOnly: true, Secure: secureRequest(r), SameSite: http.SameSiteStrictMode, MaxAge: int(sessionLifetime.Seconds()), Expires: result.expires})
	setWebIdentity(r, "session", s.config.User)
	recordWebAuth(r, "login", "success", "", s.config.User)
	writeJSON(w, http.StatusOK, map[string]any{"user": s.config.User, "expiresAt": result.expires})
}

type loginResult struct {
	status  int
	reason  string
	message string
	value   string
	expires time.Time
}

func (s *Server) authenticateLogin(host, user, password, previousSession string) loginResult {
	s.auth.mu.Lock()
	defer s.auth.mu.Unlock()
	now := time.Now()
	for ip, attempt := range s.auth.failures {
		if !now.Before(attempt.expires) {
			delete(s.auth.failures, ip)
		}
	}
	limited := loginResult{status: http.StatusTooManyRequests, reason: "rate_limited", message: "too many login attempts; try again later"}
	attempt := s.auth.failures[host]
	if attempt.count >= 10 {
		return limited
	}
	validUser := auth.Equal(user, s.config.User)
	validPassword := auth.Equal(password, s.config.Password)
	if !validUser || !validPassword {
		if len(s.auth.failures) >= sessionLimit && attempt.count == 0 {
			return limited
		}
		if attempt.count == 0 {
			attempt.expires = now.Add(5 * time.Minute)
		}
		attempt.count++
		s.auth.failures[host] = attempt
		return loginResult{status: http.StatusUnauthorized, reason: "invalid_credentials", message: "invalid username or password"}
	}
	delete(s.auth.failures, host)
	for key, session := range s.auth.sessions {
		if !now.Before(session.expires) {
			close(session.done)
			delete(s.auth.sessions, key)
		}
	}
	// Reauthentication rotates the old session, including its SSE connections.
	if previousSession != "" {
		key := sha256.Sum256([]byte(previousSession))
		if session := s.auth.sessions[key]; session != nil {
			close(session.done)
			delete(s.auth.sessions, key)
		}
	}
	if len(s.auth.sessions) >= sessionLimit {
		return loginResult{status: http.StatusServiceUnavailable, reason: "session_capacity", message: "session capacity reached; try again later"}
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return loginResult{status: http.StatusInternalServerError, reason: "session_creation_failed", message: "could not create session"}
	}
	value := hex.EncodeToString(secret)
	session := &browserSession{expires: now.Add(sessionLifetime), done: make(chan struct{})}
	s.auth.sessions[sha256.Sum256([]byte(value))] = session
	return loginResult{status: http.StatusOK, value: value, expires: session.expires}
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	session := s.auth.session(r)
	if session == nil {
		writeError(w, http.StatusUnauthorized, "login required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": s.config.User, "expiresAt": session.expires})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	s.auth.revoke(r)
	recordWebAuth(r, "logout", "success", "", s.config.User)
	http.SetCookie(w, &http.Cookie{Name: sessionCookieName, Value: "", Path: "/", HttpOnly: true, Secure: secureRequest(r), SameSite: http.SameSiteStrictMode, MaxAge: -1, Expires: time.Unix(1, 0)})
	w.WriteHeader(http.StatusNoContent)
}
