package controller

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type blockedLoginWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
}

func (w *blockedLoginWriter) WriteHeader(code int) {
	close(w.entered)
	<-w.release
	w.ResponseRecorder.WriteHeader(code)
}

func TestBlockedLoginResponseDoesNotBlockOtherSessions(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
			cookie := loginCookie(t, s)
			body := `{"user":"admin","password":"secret"}`
			if status == http.StatusUnauthorized {
				body = `{"user":"admin","password":"wrong"}`
			}
			if status == http.StatusTooManyRequests {
				s.auth.failures["192.0.2.1"] = loginAttempts{count: 10, expires: time.Now().Add(time.Minute)}
			}
			if status == http.StatusServiceUnavailable {
				for i := 1; i < sessionLimit; i++ {
					key := sha256.Sum256([]byte(fmt.Sprint(i)))
					s.auth.sessions[key] = &browserSession{expires: time.Now().Add(sessionLifetime), done: make(chan struct{})}
				}
			}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			writer := &blockedLoginWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
			loginDone := make(chan struct{})
			go func() {
				s.handleLogin(writer, request)
				close(loginDone)
			}()
			<-writer.entered
			sessionRequest := httptest.NewRequest(http.MethodGet, "/api/v1/auth/session", nil)
			sessionRequest.AddCookie(cookie)
			sessionDone := make(chan struct{})
			sessionResponse := httptest.NewRecorder()
			go func() {
				s.handleSession(sessionResponse, sessionRequest)
				s.auth.revoke(sessionRequest)
				close(sessionDone)
			}()
			select {
			case <-sessionDone:
			case <-time.After(2 * time.Second):
				t.Error("a stalled login response blocked session validation/logout")
			}
			close(writer.release)
			<-loginDone
			<-sessionDone
			if writer.Code != status || sessionResponse.Code != http.StatusOK {
				t.Fatalf("login/session status = %d/%d, want %d/200", writer.Code, sessionResponse.Code, status)
			}
		})
	}
}
