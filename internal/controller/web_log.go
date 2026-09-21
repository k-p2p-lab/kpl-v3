package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const webLogMaxBytes = 10 << 20
const webLogBackups = 5

type webLogs struct {
	access   *rotatingWebLog
	auth     *rotatingWebLog
	sequence atomic.Uint64
}

type rotatingWebLog struct {
	mu            sync.Mutex
	dataDir, name string
	maxBytes      int64
	backups       int
}

func newWebLogs(dataDir string) *webLogs {
	return &webLogs{
		access: &rotatingWebLog{dataDir: dataDir, name: "access.jsonl", maxBytes: webLogMaxBytes, backups: webLogBackups},
		auth:   &rotatingWebLog{dataDir: dataDir, name: "auth.jsonl", maxBytes: webLogMaxBytes, backups: webLogBackups},
	}
}

func (l *rotatingWebLog) directory() (*os.Root, error) {
	if err := os.MkdirAll(l.dataDir, 0755); err != nil {
		return nil, err
	}
	data, err := os.OpenRoot(l.dataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	if err := data.Mkdir("logs", 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	root, err := openResultDirectory(data, "logs")
	if err != nil {
		return nil, err
	}
	if err := root.Chmod(".", 0700); err != nil {
		_ = root.Close()
		return nil, err
	}
	return root, nil
}

func (l *rotatingWebLog) open(root *os.Root) (*os.File, int64, error) {
	before, err := root.Lstat(l.name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, 0, err
	}
	if err == nil && !before.Mode().IsRegular() {
		return nil, 0, errors.New("web log must be a regular file")
	}
	file, err := root.OpenFile(l.name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err == nil && (!info.Mode().IsRegular() || before != nil && !os.SameFile(before, info)) {
		err = errors.New("web log changed while opening")
	}
	if err == nil {
		err = file.Chmod(0600)
	}
	if err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	return file, info.Size(), nil
}

func (l *rotatingWebLog) prepare() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	root, err := l.directory()
	if err != nil {
		return err
	}
	defer root.Close()
	file, _, err := l.open(root)
	if err != nil {
		return err
	}
	return file.Close()
}

func (l *rotatingWebLog) append(record webLogRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	root, err := l.directory()
	if err != nil {
		return err
	}
	defer root.Close()
	file, size, err := l.open(root)
	if err != nil {
		return err
	}
	if size > 0 && size+int64(len(raw)) > l.maxBytes {
		if err := file.Close(); err != nil {
			return err
		}
		if err := root.Remove(fmt.Sprintf("%s.%d", l.name, l.backups)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		for i := l.backups - 1; i >= 1; i-- {
			if err := root.Rename(fmt.Sprintf("%s.%d", l.name, i), fmt.Sprintf("%s.%d", l.name, i+1)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err := root.Rename(l.name, l.name+".1"); err != nil {
			return err
		}
		file, size, err = l.open(root)
		if err != nil {
			return err
		}
	}
	n, err := file.Write(raw)
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	if err != nil {
		// Keep a failed partial write from corrupting the next JSON line.
		err = errors.Join(err, file.Truncate(size))
	}
	return errors.Join(err, file.Close())
}

type webLogRecord struct {
	Timestamp      time.Time `json:"timestamp"`
	Event          string    `json:"event"`
	RequestID      string    `json:"requestId"`
	RemoteIP       string    `json:"remoteIp"`
	Method         string    `json:"method"`
	Path           string    `json:"path"`
	Status         int       `json:"status"`
	Bytes          int64     `json:"bytes"`
	DurationMS     float64   `json:"durationMs"`
	UserAgent      string    `json:"userAgent,omitempty"`
	Authentication string    `json:"authentication"`
	User           string    `json:"user,omitempty"`
	Outcome        string    `json:"outcome"`
	Reason         string    `json:"reason,omitempty"`
}

type webAuditKey struct{}
type webAuditRequest struct {
	authentication, user                  string
	event, outcome, reason, attemptedUser string
}

func setWebIdentity(r *http.Request, kind, user string) {
	if audit, ok := r.Context().Value(webAuditKey{}).(*webAuditRequest); ok {
		audit.authentication, audit.user = kind, user
	}
}

func recordWebAuth(r *http.Request, event, outcome, reason, user string) {
	if audit, ok := r.Context().Value(webAuditKey{}).(*webAuditRequest); ok {
		if event != "" {
			audit.event = event
		}
		if audit.event == "" {
			audit.event = "access_denied"
		}
		audit.outcome, audit.reason, audit.attemptedUser = outcome, reason, user
	}
}

func boundedWebField(value string, limit int) string {
	if len(value) > limit {
		return value[:limit]
	}
	return value
}

func (s *Server) appendWebLog(log *rotatingWebLog, record webLogRecord) {
	if err := log.append(record); err != nil {
		s.logger.Error("write web audit log", "file", log.name, "error", err)
	}
}

func omitRoutineWebAccess(r *http.Request, audit *webAuditRequest, status int) bool {
	if status >= 400 {
		return false
	}
	if audit.authentication == "internal" {
		return true
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		switch r.URL.Path {
		case "/api/v1/health", "/metrics", "/api/v1/prometheus/agent-targets", "/api/v1/prometheus/controller-targets":
			return true
		}
	}
	return false
}

func (s *Server) withWebLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		audit := &webAuditRequest{authentication: "anonymous"}
		if r.Method == http.MethodPost {
			switch r.URL.Path {
			case "/api/v1/auth/login":
				audit.event = "login"
			case "/api/v1/auth/logout":
				audit.event = "logout"
			}
		}
		r = r.WithContext(context.WithValue(r.Context(), webAuditKey{}, audit))
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		// Do not copy query strings, bodies, cookies, Authorization, or Referer.
		// Forwarding headers are client-controlled without a trusted proxy list;
		// record the actual TCP peer address instead.
		base := webLogRecord{
			RequestID: fmt.Sprintf("%x-%x", start.UnixNano(), s.webLogs.sequence.Add(1)),
			RemoteIP:  boundedWebField(ip, 128), Method: boundedWebField(r.Method, 32),
			Path: boundedWebField(r.URL.EscapedPath(), 2048), UserAgent: boundedWebField(r.UserAgent(), 512),
		}
		response := &webLogResponse{ResponseWriter: w}
		streamOpened := false
		response.onHeader = func(status int) {
			if r.URL.Path == "/api/v1/stream" && status == http.StatusOK {
				streamOpened = true
				record := base
				record.Timestamp, record.Event, record.Status = time.Now().UTC(), "sse_open", status
				record.Authentication, record.User, record.Outcome = audit.authentication, boundedWebField(audit.user, 128), "success"
				s.appendWebLog(s.webLogs.access, record)
			}
		}
		completed := false
		defer func() {
			status := response.status
			if status == 0 {
				status = http.StatusOK
				if !completed {
					status = http.StatusInternalServerError
				}
			}
			record := base
			record.Timestamp, record.Event, record.Status = time.Now().UTC(), "http_access", status
			if streamOpened {
				record.Event = "sse_close"
			}
			record.Bytes, record.DurationMS = response.bytes, float64(time.Since(start).Microseconds())/1000
			record.Authentication, record.User = audit.authentication, boundedWebField(audit.user, 128)
			record.Outcome, record.Reason = "success", audit.reason
			if status >= 400 {
				record.Outcome = "failure"
			}
			if !completed {
				record.Outcome, record.Reason = "aborted", "handler_aborted"
			}
			if !omitRoutineWebAccess(r, audit, status) || !completed {
				s.appendWebLog(s.webLogs.access, record)
			}
			if audit.event != "" {
				record.Event, record.User = audit.event, boundedWebField(audit.attemptedUser, 128)
				record.Outcome = audit.outcome
				if record.Outcome == "" {
					record.Outcome = "failure"
				}
				s.appendWebLog(s.webLogs.auth, record)
			}
		}()
		var wrapped http.ResponseWriter = response
		if _, ok := w.(http.Flusher); ok {
			wrapped = &flushingWebLogResponse{response}
		}
		next.ServeHTTP(wrapped, r)
		completed = true
	})
}

// Unwrap preserves ResponseController deadlines. Only advertise Flusher when
// the underlying writer supports it, so SSE and streaming errors keep working.
type webLogResponse struct {
	http.ResponseWriter
	status   int
	bytes    int64
	onHeader func(int)
}

func (w *webLogResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *webLogResponse) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
	if w.onHeader != nil {
		w.onHeader(status)
	}
}
func (w *webLogResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += int64(n)
	return n, err
}

type flushingWebLogResponse struct{ *webLogResponse }

func (w *flushingWebLogResponse) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *flushingWebLogResponse) Flush() { _ = w.FlushError() }
