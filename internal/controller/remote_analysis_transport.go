package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const (
	remoteAnalysisProtocol     = 1
	remoteAnalysisPath         = "/internal/result-analysis"
	remoteAnalysisIdlePath     = "/internal/analysis-idle"
	remoteAnalysisWriteTimeout = 15 * time.Second
	remoteAnalysisReadTimeout  = 60 * time.Second
)

var (
	errRemoteAnalysisUnavailable        = errors.New("analysis worker unavailable")
	errRemoteAnalysisCleanupUnconfirmed = errors.New("analysis worker cleanup could not be confirmed")
)

type remoteAnalysisHello struct {
	Protocol        int                  `json:"protocol"`
	AnalysisVersion int                  `json:"analysisVersion"`
	Build           string               `json:"build"`
	Input           analysisProcessInput `json:"input"`
}

// Control frames are JSON. Source and artifact data follow their descriptor in
// a binary frame to keep the data path bounded and avoid base64 on the network.
type remoteAnalysisFrame struct {
	Method string          `json:"method"`
	Body   json.RawMessage `json:"body,omitempty"`
	Bytes  int             `json:"bytes,omitempty"`
	SHA256 string          `json:"sha256,omitempty"`
	data   []byte
}

type remoteAnalysisOutput struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
}

type remoteAnalysisSocket struct {
	conn      *websocket.Conn
	ctx       context.Context
	cancel    context.CancelCauseFunc
	stopClose func() bool
	in        chan remoteAnalysisFrame
	done      chan struct{}
}

func openRemoteAnalysisSocket(ctx context.Context, conn *websocket.Conn, cancelAnalysis func()) *remoteAnalysisSocket {
	ctx, cancel := context.WithCancelCause(ctx)
	s := &remoteAnalysisSocket{conn: conn, ctx: ctx, cancel: cancel, in: make(chan remoteAnalysisFrame), done: make(chan struct{})}
	s.stopClose = context.AfterFunc(ctx, func() { _ = conn.Close() })
	conn.SetReadLimit(workerproc.MaxMessageBytes)
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(remoteAnalysisReadTimeout))
	})
	go func() {
		defer close(s.done)
		if err := conn.SetReadDeadline(time.Now().Add(remoteAnalysisReadTimeout)); err != nil {
			cancel(err)
			return
		}
		for {
			frame, err := readRemoteAnalysisFrame(conn)
			if err != nil {
				cancel(err)
				return
			}
			if frame.Method == "cancel" && cancelAnalysis != nil {
				if frame.Bytes != 0 {
					cancel(errors.New("invalid remote analysis cancellation"))
					return
				}
				cancelAnalysis()
				continue
			}
			select {
			case s.in <- frame:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(remoteAnalysisReadTimeout / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(remoteAnalysisWriteTimeout)); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	return s
}

func (s *remoteAnalysisSocket) close() {
	s.cancel(context.Canceled)
	_ = s.conn.Close()
	s.stopClose()
	<-s.done
}

func readRemoteAnalysisFrame(conn *websocket.Conn) (remoteAnalysisFrame, error) {
	var frame remoteAnalysisFrame
	kind, data, err := conn.ReadMessage()
	if err != nil {
		return frame, err
	}
	if kind != websocket.TextMessage || json.Unmarshal(data, &frame) != nil || frame.Method == "" ||
		frame.Bytes < 0 || frame.Bytes > workerSourceChunk {
		return frame, errors.New("invalid remote analysis frame")
	}
	if frame.Bytes == 0 {
		if frame.SHA256 != "" {
			return frame, errors.New("unexpected remote analysis frame checksum")
		}
		return frame, nil
	}
	kind, payload, err := conn.NextReader()
	if err != nil {
		return frame, err
	}
	if kind != websocket.BinaryMessage || len(frame.SHA256) != sha256.Size*2 {
		return frame, errors.New("invalid remote analysis binary frame")
	}
	frame.data = make([]byte, frame.Bytes)
	if _, err := io.ReadFull(payload, frame.data); err != nil {
		return frame, fmt.Errorf("remote analysis binary frame length mismatch: %w", err)
	}
	var tail [1]byte
	if n, err := io.ReadFull(payload, tail[:]); n != 0 || !errors.Is(err, io.EOF) {
		return frame, errors.New("remote analysis binary frame length mismatch")
	}
	sum := sha256.Sum256(frame.data)
	if frame.SHA256 != hex.EncodeToString(sum[:]) {
		return frame, errors.New("remote analysis binary frame checksum mismatch")
	}
	return frame, nil
}

func (s *remoteAnalysisSocket) receive(ctx context.Context) (remoteAnalysisFrame, error) {
	select {
	case frame := <-s.in:
		return frame, nil
	case <-ctx.Done():
		return remoteAnalysisFrame{}, context.Cause(ctx)
	case <-s.ctx.Done():
		return remoteAnalysisFrame{}, context.Cause(s.ctx)
	}
}

// Each side has one writer for application messages. The reader and pings may
// run concurrently, as permitted by the WebSocket connection.
func (s *remoteAnalysisSocket) send(method string, value any, data []byte) error {
	if err := s.ctx.Err(); err != nil {
		return context.Cause(s.ctx)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	header := remoteAnalysisFrame{Method: method, Body: body, Bytes: len(data)}
	if len(data) > 0 {
		sum := sha256.Sum256(data)
		header.SHA256 = hex.EncodeToString(sum[:])
	}
	frame, err := json.Marshal(header)
	if err != nil {
		return err
	}
	if len(frame) > workerproc.MaxMessageBytes || len(data) > workerSourceChunk {
		return errors.New("remote analysis message exceeds size limit")
	}
	if err := s.conn.SetWriteDeadline(time.Now().Add(remoteAnalysisWriteTimeout)); err != nil {
		return err
	}
	if err := s.conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		return err
	}
	if len(data) > 0 {
		return s.conn.WriteMessage(websocket.BinaryMessage, data)
	}
	return nil
}

func remoteAnalysisURL(endpoint string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") {
		return nil, errors.New("analysis worker URL must be an http or https origin")
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return nil, errors.New("analysis worker URL must be an http or https origin")
	}
	u.Path, u.RawPath = remoteAnalysisPath, ""
	return u, nil
}

func ValidateAnalysisWorkerToken(token string) error {
	if len(token) < 32 || len(token) > 256 || strings.IndexFunc(token, func(r rune) bool {
		return r < '!' || r > '~'
	}) >= 0 {
		return errors.New("analysis worker token must contain 32 to 256 printable non-space ASCII characters")
	}
	return nil
}

func ValidateAnalysisWorkerConnections(endpoints []string, token string) error {
	if len(endpoints) == 0 {
		if token == "" {
			return nil
		}
		return errors.New("analysis worker token requires at least one analysis worker URL")
	}
	if len(endpoints) > 16 {
		return errors.New("configure at most 16 analysis worker URLs")
	}
	if err := ValidateAnalysisWorkerToken(token); err != nil {
		return err
	}
	seen := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		u, err := remoteAnalysisURL(endpoint)
		if err != nil {
			return err
		}
		port := u.Port()
		if port == "" {
			port = "80"
			if u.Scheme == "wss" {
				port = "443"
			}
		}
		origin := u.Scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
		if seen[origin] {
			return errors.New("analysis worker URLs must have distinct origins")
		}
		seen[origin] = true
	}
	return nil
}

func remoteAnalysisReadBoundary(input analysisProcessInput, read workerSourceRead) (int, bool, error) {
	if read.Snapshot < 0 || read.Snapshot >= len(input.Snapshots) ||
		read.Offset < 0 || read.Length < 1 || read.Length > workerSourceChunk {
		return 0, false, errors.New("invalid remote analysis source request")
	}
	files := input.Snapshots[read.Snapshot].Files
	if read.File < 0 || read.File >= len(files) || files[read.File].Size < 0 || !slices.Contains(analysisSourceNames, files[read.File].Name) {
		return 0, false, errors.New("invalid remote analysis source file")
	}
	remaining := files[read.File].Size - read.Offset
	if remaining <= 0 {
		return 0, true, nil
	}
	return int(min(remaining, int64(read.Length))), remaining <= int64(read.Length), nil
}

func dialRemoteAnalysisWorker(ctx context.Context, endpoint, token string) (*websocket.Conn, error) {
	target, err := remoteAnalysisURL(endpoint)
	if err != nil {
		return nil, err
	}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	deadline := time.Now().Add(8 * time.Second)
	for {
		conn, response, err := dialer.DialContext(ctx, target.String(), http.Header{"Authorization": {"Bearer " + token}})
		if err == nil {
			return conn, nil
		}
		if response == nil {
			return nil, errors.Join(errRemoteAnalysisUnavailable, err)
		}
		_ = response.Body.Close()
		switch response.StatusCode {
		case http.StatusTooManyRequests:
			// A disconnected job may still be reaping its private calculation.
			// Retry admission only, before this request starts any calculation.
			if !time.Now().Before(deadline) {
				return nil, fmt.Errorf("%w: still busy", errRemoteAnalysisUnavailable)
			}
			if err := sleepContext(ctx, 250*time.Millisecond); err != nil {
				return nil, err
			}
		case http.StatusServiceUnavailable:
			return nil, errRemoteAnalysisUnavailable
		default:
			return nil, fmt.Errorf("analysis worker refused connection: HTTP %d", response.StatusCode)
		}
	}
}

func (s *remoteAnalysisSocket) cancelAndWait() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sendErr := s.send("cancel", nil, nil)
	for {
		frame, err := s.receive(ctx)
		if err != nil {
			return errors.Join(sendErr, err)
		}
		// An in-flight callback may precede the terminal frame. The Worker
		// sends its terminal response only after collecting its process.
		if frame.Bytes == 0 && (frame.Method == "result" || frame.Method == "error") {
			return nil
		}
	}
}

var remoteAnalysisProbeClient = &http.Client{
	Transport:     &http.Transport{DisableKeepAlives: true},
	Timeout:       3 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func probeRemoteAnalysisIdle(ctx context.Context, endpoint, token string) error {
	u, err := remoteAnalysisURL(endpoint)
	if err != nil {
		return err
	}
	if u.Scheme == "wss" {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	u.Path = remoteAnalysisIdlePath
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := remoteAnalysisProbeClient.Do(request)
	if err != nil {
		return err
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("analysis worker idle probe: HTTP %d", response.StatusCode)
	}
	return nil
}

func computeRemoteAnalysis(ctx context.Context, endpoint, token string, input analysisProcessInput, root *os.Root, source workerproc.Handler, progress func(analysisProcessProgress), metadata workerproc.Handler, transferRate uint64) (_ *workerAnalysisArtifacts, result analysisProcessResult, finalErr error) {
	conn, err := dialRemoteAnalysisWorker(ctx, endpoint, token)
	if err != nil {
		return nil, result, err
	}
	if transferRate == 0 {
		transferRate = defaultAnalysisTransferMiBPerSecond
	}
	// Keep the control connection alive during cancellation so the remote
	// worker can confirm process cleanup before the shared queue advances.
	socket := openRemoteAnalysisSocket(context.WithoutCancel(ctx), conn, nil)
	defer socket.close()
	operation, stopOperation := context.WithCancelCause(ctx)
	defer stopOperation(nil)
	stopDisconnect := context.AfterFunc(socket.ctx, func() { stopOperation(context.Cause(socket.ctx)) })
	defer stopDisconnect()
	build := model.BuildDetails().BinaryHash
	if build == "" {
		return nil, result, errors.New("cannot verify Controller binary identity")
	}
	if err := operation.Err(); err != nil {
		return nil, result, context.Cause(operation)
	}
	terminal := false
	defer func() {
		if !terminal {
			if err := socket.cancelAndWait(); err != nil {
				finalErr = errors.Join(finalErr, errRemoteAnalysisCleanupUnconfirmed, err)
			}
		}
	}()
	if err := socket.send("start", remoteAnalysisHello{Protocol: remoteAnalysisProtocol, AnalysisVersion: currentAnalysisVersion, Build: build, Input: input}, nil); err != nil {
		return nil, result, err
	}

	names := []string{"full"}
	if input.BatchID == "" {
		names = append(names, "compact")
	}
	artifacts, err := newWorkerAnalysisArtifacts(root, names...)
	if err != nil {
		return nil, result, err
	}
	keep := false
	defer func() {
		if !keep {
			artifacts.discard()
		}
	}()
	isBatch := input.BatchID != ""
	metadataSent := !isBatch
	outputStarted := false
	for {
		frame, err := socket.receive(operation)
		if err != nil {
			return nil, result, err
		}
		sourcesClosed := outputStarted || (isBatch && metadataSent)
		switch frame.Method {
		case "source.read":
			if sourcesClosed || source == nil || frame.Bytes != 0 {
				return nil, result, errors.New("unexpected remote analysis source request")
			}
			var read workerSourceRead
			if err := json.Unmarshal(frame.Body, &read); err != nil {
				return nil, result, err
			}
			length, eof, err := remoteAnalysisReadBoundary(input, read)
			if err != nil {
				return nil, result, err
			}
			value, err := source(operation, frame.Method, frame.Body)
			if err != nil {
				return nil, result, err
			}
			bytes, ok := value.(workerSourceBytes)
			if !ok || len(bytes.Data) != length || bytes.EOF != eof {
				return nil, result, errors.New("analysis source did not match its captured boundary")
			}
			err = socket.send("reply", bytes.EOF, bytes.Data)
			if err != nil {
				return nil, result, err
			}
		case "analysis.progress":
			var p analysisProcessProgress
			if sourcesClosed || frame.Bytes != 0 || json.Unmarshal(frame.Body, &p) != nil || p.Snapshot < 0 ||
				p.Snapshot >= len(input.Snapshots) || p.Bytes < 0 || len(p.Phase) > 256 {
				return nil, result, errors.New("invalid remote analysis progress")
			}
			if progress != nil {
				progress(p)
			}
			if err := socket.send("reply", nil, nil); err != nil {
				return nil, result, err
			}
		case "analysis.batch-metadata":
			if metadataSent || metadata == nil || frame.Bytes != 0 {
				return nil, result, errors.New("unexpected remote analysis metadata request")
			}
			value, err := metadata(operation, frame.Method, frame.Body)
			if err != nil {
				return nil, result, err
			}
			if err := socket.send("reply", value, nil); err != nil {
				return nil, result, err
			}
			metadataSent = true
		case "analysis.write":
			var out remoteAnalysisOutput
			if !metadataSent || frame.Bytes <= 0 || frame.Bytes > workerSourceChunk/2 || json.Unmarshal(frame.Body, &out) != nil {
				return nil, result, errors.New("invalid remote analysis output")
			}
			outputStarted = true
			started := time.Now()
			if err := artifacts.write(operation, analysisOutputChunk{Name: out.Name, Offset: out.Offset, Data: frame.data}); err != nil {
				return nil, result, err
			}
			if err := sleepContext(operation, archiveTransferDelay(len(frame.data), transferRate, time.Since(started))); err != nil {
				return nil, result, err
			}
			if err := socket.send("reply", nil, nil); err != nil {
				return nil, result, err
			}
		case "result":
			if !metadataSent || frame.Bytes != 0 || json.Unmarshal(frame.Body, &result) != nil {
				return nil, result, errors.New("invalid remote analysis completion")
			}
			terminal = true
			if err := artifacts.seal(ctx, result.Outputs); err != nil {
				return nil, result, err
			}
			keep = true
			return artifacts, result, nil
		case "error":
			var message string
			if frame.Bytes != 0 || json.Unmarshal(frame.Body, &message) != nil || len(message) > 2048 {
				return nil, result, errors.New("invalid remote analysis failure")
			}
			terminal = true
			return nil, result, fmt.Errorf("analysis worker: %s", message)
		default:
			return nil, result, errors.New("unknown remote analysis message")
		}
	}
}
