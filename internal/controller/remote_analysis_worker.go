package controller

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type remoteAnalysisWorker struct {
	ctx          context.Context
	cancel       context.CancelFunc
	tokenHash    [sha256.Size]byte
	runtimeImage string
	logger       *slog.Logger
	command      func(string) *exec.Cmd
	slot         chan struct{}
	closed       atomic.Bool
}

func newRemoteAnalysisWorker(token string, logger *slog.Logger, command func(string) *exec.Cmd) *remoteAnalysisWorker {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = slog.Default()
	}
	return &remoteAnalysisWorker{ctx: ctx, cancel: cancel, tokenHash: sha256.Sum256([]byte(token)), runtimeImage: strings.TrimSpace(os.Getenv("KPL_RUNTIME_IMAGE")), logger: logger, command: command, slot: make(chan struct{}, 1)}
}

// NewAgentAnalysisWorker serves calculation requests on the existing Agent API.
// Its stop function waits until the accepted job and its OS child are reclaimed.
func NewAgentAnalysisWorker(ctx context.Context, token, runtimeImage string, logger *slog.Logger) (http.Handler, func(context.Context) error) {
	worker := newRemoteAnalysisWorker(token, logger, nil)
	worker.runtimeImage = strings.TrimSpace(runtimeImage)
	stopParent := context.AfterFunc(ctx, worker.cancel)
	if ctx.Err() != nil {
		worker.cancel()
	}
	return worker, func(stopCtx context.Context) error {
		stopParent()
		return worker.stop(stopCtx)
	}
}

func (w *remoteAnalysisWorker) stop(ctx context.Context) error {
	w.closed.Store(true)
	w.cancel()
	select {
	case w.slot <- struct{}{}:
		<-w.slot
		return nil
	default:
	}
	select {
	case w.slot <- struct{}{}:
		<-w.slot
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var (
	analysisBinaryHash     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	analysisImmutableImage = regexp.MustCompile(`^[a-zA-Z0-9._:][a-zA-Z0-9._:/-]*@sha256:[0-9a-f]{64}$`)
)

func compatibleRemoteAnalysis(hello remoteAnalysisHello, build, image string) bool {
	if hello.Protocol != remoteAnalysisProtocol || hello.AnalysisVersion != currentAnalysisVersion ||
		!analysisBinaryHash.MatchString(build) || !analysisBinaryHash.MatchString(hello.Build) {
		return false
	}
	if hello.Build == build {
		return true
	}
	// A multi-platform image index selects different executables on each CPU
	// architecture. Its immutable deployment reference identifies their shared
	// release, while protocol and analysis versions still match explicitly.
	return hello.Input.RuntimeImage == image && analysisImmutableImage.MatchString(image) &&
		!strings.Contains(image, "//") && !strings.Contains(image, "/@")
}

func (w *remoteAnalysisWorker) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if (request.URL.Path != remoteAnalysisPath && request.URL.Path != remoteAnalysisIdlePath) || request.Method != http.MethodGet {
		http.NotFound(response, request)
		return
	}
	token, ok := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
	hash := sha256.Sum256([]byte(token))
	if !ok || subtle.ConstantTimeCompare(w.tokenHash[:], hash[:]) != 1 {
		http.Error(response, "unauthorized", http.StatusUnauthorized)
		return
	}
	select {
	case w.slot <- struct{}{}:
		defer func() { <-w.slot }()
	case <-request.Context().Done():
		return
	default:
		http.Error(response, "analysis worker busy", http.StatusTooManyRequests)
		return
	}
	if w.closed.Load() || w.ctx.Err() != nil || request.Context().Err() != nil {
		http.Error(response, "analysis worker stopping", http.StatusServiceUnavailable)
		return
	}
	if request.URL.Path == remoteAnalysisIdlePath {
		response.WriteHeader(http.StatusNoContent)
		return
	}
	upgrader := websocket.Upgrader{HandshakeTimeout: 5 * time.Second}
	conn, err := upgrader.Upgrade(response, request, nil)
	if err != nil {
		return
	}
	connectionCtx, cancelConnection := context.WithCancel(request.Context())
	defer cancelConnection()
	stopWorker := context.AfterFunc(w.ctx, cancelConnection)
	defer stopWorker()
	jobCtx, cancelJob := context.WithCancel(connectionCtx)
	defer cancelJob()
	socket := openRemoteAnalysisSocket(connectionCtx, conn, cancelJob)
	defer socket.close()
	stopDisconnect := context.AfterFunc(socket.ctx, cancelJob)
	defer stopDisconnect()
	process, result, err := w.calculate(jobCtx, socket)
	if process != nil {
		// An interrupted socket must not release its worker slot while any
		// child or private callback is still alive. The terminal frame is
		// the Controller's proof that this calculation stopped.
		<-process.Done()
	}
	if err != nil {
		message := err.Error()
		if len(message) > 2048 {
			message = strings.ToValidUTF8(message[:2048], "")
		}
		_ = socket.send("error", message, nil)
		return
	}
	_ = socket.send("result", result, nil)
}

func (w *remoteAnalysisWorker) calculate(jobCtx context.Context, socket *remoteAnalysisSocket) (*workerproc.Process, analysisProcessResult, error) {
	var result analysisProcessResult
	handshake, cancel := context.WithTimeout(jobCtx, 10*time.Second)
	frame, err := socket.receive(handshake)
	cancel()
	if err != nil {
		return nil, result, err
	}
	var hello remoteAnalysisHello
	build := model.BuildDetails().BinaryHash
	if frame.Method != "start" || frame.Bytes != 0 || json.Unmarshal(frame.Body, &hello) != nil ||
		!compatibleRemoteAnalysis(hello, build, w.runtimeImage) {
		return nil, result, errors.New("incompatible remote analysis request")
	}
	var mu sync.Mutex
	exchange := func(ctx context.Context, method string, value any, data []byte) (remoteAnalysisFrame, error) {
		if err := ctx.Err(); err != nil {
			return remoteAnalysisFrame{}, err
		}
		if err := socket.send(method, value, data); err != nil {
			return remoteAnalysisFrame{}, err
		}
		reply, err := socket.receive(ctx)
		if err != nil {
			// No reply from an interrupted exchange belongs to a later call.
			if jobCtx.Err() == nil {
				socket.cancel(err)
			}
			return remoteAnalysisFrame{}, err
		}
		if reply.Method != "reply" {
			return remoteAnalysisFrame{}, errors.New("invalid remote analysis reply")
		}
		if method != "source.read" && reply.Bytes != 0 {
			return remoteAnalysisFrame{}, errors.New("unexpected binary remote analysis reply")
		}
		return reply, nil
	}
	var bufferedRead workerSourceRead
	var readBuffer []byte
	var pendingProgress analysisProcessProgress
	var lastProgress time.Time
	flushProgress := func(ctx context.Context) error {
		if _, err := exchange(ctx, "analysis.progress", pendingProgress, nil); err != nil {
			return err
		}
		pendingProgress.Bytes = 0
		lastProgress = time.Now()
		return nil
	}
	broker := func(ctx context.Context, method string, body json.RawMessage) (any, error) {
		// The private child can have concurrent callbacks. Each remote exchange
		// and its single-block read cache stay serialized within this job.
		mu.Lock()
		defer mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch method {
		case "source.read":
			var read workerSourceRead
			if err := json.Unmarshal(body, &read); err != nil {
				return nil, err
			}
			length, eof, err := remoteAnalysisReadBoundary(hello.Input, read)
			if err != nil || length == 0 {
				return workerSourceBytes{EOF: eof}, err
			}
			start := read.Offset - bufferedRead.Offset
			if read.Snapshot == bufferedRead.Snapshot && read.File == bufferedRead.File &&
				start >= 0 && start <= int64(len(readBuffer))-int64(length) {
				return workerSourceBytes{Data: readBuffer[start : start+int64(length)], EOF: eof}, nil
			}
			// Sequential log scans request 64 KiB blocks. Fetch ahead up to the
			// existing 1 MiB source cap and leave small random samples alone.
			fetch := read
			if fetch.Length >= 64<<10 {
				fetch.Length = workerSourceChunk
			}
			fetchLength, fetchEOF, err := remoteAnalysisReadBoundary(hello.Input, fetch)
			if err != nil {
				return nil, err
			}
			reply, err := exchange(ctx, method, fetch, nil)
			if err != nil {
				return nil, err
			}
			var actualEOF bool
			if json.Unmarshal(reply.Body, &actualEOF) != nil || actualEOF != fetchEOF || len(reply.data) != fetchLength {
				return nil, errors.New("invalid remote analysis source block")
			}
			bufferedRead, readBuffer = fetch, reply.data
			return workerSourceBytes{Data: readBuffer[:length], EOF: eof}, nil
		case "analysis.progress":
			var update analysisProcessProgress
			if json.Unmarshal(body, &update) != nil || update.Bytes < 0 || update.Bytes > workerproc.MaxMessageBytes {
				return nil, errors.New("invalid analysis progress update")
			}
			if lastProgress.IsZero() || update.Snapshot != pendingProgress.Snapshot || update.Phase != pendingProgress.Phase {
				if pendingProgress.Bytes != 0 {
					if err := flushProgress(ctx); err != nil {
						return nil, err
					}
				}
				pendingProgress = update
				return nil, flushProgress(ctx)
			}
			pendingProgress.Bytes += update.Bytes
			if pendingProgress.Bytes >= workerSourceChunk || time.Since(lastProgress) >= time.Second {
				return nil, flushProgress(ctx)
			}
			return nil, nil
		case "analysis.batch-metadata":
			if pendingProgress.Bytes != 0 {
				if err := flushProgress(ctx); err != nil {
					return nil, err
				}
			}
			reply, err := exchange(ctx, method, body, nil)
			return reply.Body, err
		case "analysis.write":
			if pendingProgress.Bytes != 0 {
				if err := flushProgress(ctx); err != nil {
					return nil, err
				}
			}
			var chunk analysisOutputChunk
			if err := json.Unmarshal(body, &chunk); err != nil {
				return nil, err
			}
			_, err := exchange(ctx, method, remoteAnalysisOutput{Name: chunk.Name, Offset: chunk.Offset}, chunk.Data)
			return nil, err
		default:
			return nil, invalidAnalysisOperation(method)
		}
	}
	process, err := workerproc.Start(jobCtx, "controller-analysis", hello.Input, broker, workerproc.Options{Command: w.command})
	if err != nil {
		return process, result, err
	}
	w.logger.Info("remote analysis calculation started", "batchId", hello.Input.BatchID, "analysisId", hello.Input.AnalysisID, "workerPid", process.PID())
	computeErr := process.Call(jobCtx, "analyze", nil, &result)
	stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	return process, result, errors.Join(computeErr, process.CloseContext(stopCtx))
}

func RunAnalysisWorker(ctx context.Context, listen, token string, logger *slog.Logger) error {
	if err := ValidateAnalysisWorkerToken(token); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker, stopWorker := NewAgentAnalysisWorker(ctx, token, os.Getenv("KPL_RUNTIME_IMAGE"), logger)
	server := &http.Server{Addr: listen, Handler: worker, ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 16 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	if ctx.Err() != nil {
		return nil
	}
	logger.Info("analysis worker listening", "address", listen)
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		err = nil
	}
	cancel()
	// HTTP Server.Close does not wait for upgraded WebSocket connections.
	shutdown, stopShutdown := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopShutdown()
	return errors.Join(err, stopWorker(shutdown))
}
