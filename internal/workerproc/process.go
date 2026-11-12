package workerproc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type Options struct {
	RunID            string
	Command          func(kind string) *exec.Cmd
	Stdout           io.Writer
	Stderr           io.Writer
	HandshakeTimeout time.Duration
	GracePeriod      time.Duration
	KillWait         time.Duration
	MaxProcs         int   // Zero inherits the current environment.
	MemoryLimitBytes int64 // Zero inherits the current environment; this is a Go GC soft target.
}

type Process struct {
	peer     *Peer
	cmd      *exec.Cmd
	done     chan struct{}
	osDone   chan struct{}
	waitErr  error
	stopOne  sync.Once
	stopped  chan struct{}
	stopErr  error
	grace    time.Duration
	killWait time.Duration
	termSent atomic.Bool
	killSent atomic.Bool
	signalMu sync.Mutex
}

// Start can return both a Process and an error when initialization fails and
// cleanup has not reached Done. The caller must retain that process as a work
// barrier until it is reaped and its parent RPC handlers have finished.
func Start(ctx context.Context, kind string, bootstrap any, parent Handler, options ...Options) (*Process, error) {
	if ctx == nil || ctx.Err() != nil {
		if ctx != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("private worker Start requires a context")
	}
	if kind == "" || len(kind) > 64 || strings.ContainsAny(kind, " \r\n\t\x00") {
		return nil, fmt.Errorf("%w: invalid worker kind", ErrProtocol)
	}
	if len(options) > 1 {
		return nil, errors.New("private worker Start accepts one Options value")
	}
	var opts Options
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.MaxProcs < 0 || opts.MemoryLimitBytes < 0 {
		return nil, errors.New("private worker resource settings must not be negative")
	}
	body, err := marshalBody(bootstrap)
	if err != nil {
		return nil, err
	}
	if opts.RunID == "" {
		var header struct {
			RunID string `json:"runId"`
		}
		if err := json.Unmarshal(body, &header); err != nil {
			return nil, fmt.Errorf("private worker bootstrap must contain runId: %w", err)
		}
		opts.RunID = header.RunID
	}
	if opts.RunID == "" || len(opts.RunID) > 256 || strings.ContainsAny(opts.RunID, "\r\n\x00") {
		return nil, fmt.Errorf("%w: bootstrap needs a valid runId", ErrProtocol)
	}
	build := model.BuildDetails().BinaryHash
	if build == "" {
		return nil, errors.New("cannot verify current executable identity for private worker")
	}
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	id := identity{Version: ProtocolVersion, Kind: kind, RunID: opts.RunID, Epoch: hex.EncodeToString(nonce[:]), Build: build}
	hello := frame{identity: id, Type: "hello", Body: body}
	if _, err := encodeFrame(hello); err != nil {
		return nil, err
	}
	toChild, parentWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	parentRead, fromChild, err := os.Pipe()
	if err != nil {
		toChild.Close()
		parentWrite.Close()
		return nil, err
	}
	closeFiles := func() {
		toChild.Close()
		parentWrite.Close()
		parentRead.Close()
		fromChild.Close()
	}
	var cmd *exec.Cmd
	if opts.Command != nil {
		cmd = opts.Command(kind)
	} else {
		executable, err := currentExecutable()
		if err != nil {
			closeFiles()
			return nil, err
		}
		cmd = exec.Command(executable, "run", "internal-worker", kind)
	}
	if cmd == nil || len(cmd.ExtraFiles) != 0 {
		closeFiles()
		return nil, errors.New("private worker command must not supply inherited files")
	}
	cmd.ExtraFiles = []*os.File{toChild, fromChild}
	cmd.Env = privateEnvironment(cmd.Env)
	if opts.MaxProcs > 0 {
		lanes := min(opts.MaxProcs, max(1, runtime.NumCPU()), max(1, runtime.GOMAXPROCS(0)))
		cmd.Env = replacePrivateSetting(cmd.Env, "GOMAXPROCS", strconv.Itoa(lanes))
	}
	if opts.MemoryLimitBytes > 0 {
		cmd.Env = replacePrivateSetting(cmd.Env, "GOMEMLIMIT", strconv.FormatInt(opts.MemoryLimitBytes, 10)+"B")
	}
	if opts.Stdout != nil {
		cmd.Stdout = opts.Stdout
	} else if cmd.Stdout == nil {
		cmd.Stdout = os.Stdout
	}
	if opts.Stderr != nil {
		cmd.Stderr = opts.Stderr
	} else if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	cmd.Stdin = nil
	cmd.WaitDelay = time.Second
	configureProcess(cmd)
	started, exited := make(chan error, 1), make(chan error, 1)
	go func() {
		// Linux parent-death signals refer to the creating OS thread. Keep it
		// alive through Wait so normal Go thread retirement is not parent death.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		err := cmd.Start()
		started <- err
		if err != nil {
			return
		}
		err = cmd.Wait()
		cleanupProcessGroup(cmd)
		exited <- err
	}()
	if err := <-started; err != nil {
		closeFiles()
		return nil, fmt.Errorf("start private worker: %w", err)
	}
	toChild.Close()
	fromChild.Close()
	grace, killWait := opts.GracePeriod, opts.KillWait
	if grace <= 0 {
		grace = 5 * time.Second
	}
	if killWait <= 0 {
		killWait = 5 * time.Second
	}
	p := &Process{cmd: cmd, done: make(chan struct{}), osDone: make(chan struct{}), stopped: make(chan struct{}), grace: grace, killWait: killWait}
	p.peer = newPeer(context.Background(), id, parentRead, parentWrite, parent)
	go func() {
		p.waitErr = <-exited
		close(p.osDone)
		p.peer.shutdown(ErrClosed)
		<-p.peer.stopped
		close(p.done)
	}()
	go func() {
		select {
		case <-ctx.Done():
			p.startStop()
		case <-p.done:
		}
	}()
	wait := opts.HandshakeTimeout
	if wait <= 0 {
		wait = 10 * time.Second
	}
	handshake, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	if err := p.peer.send(handshake, hello, false); err == nil {
		select {
		case <-p.peer.ready:
			if err := ctx.Err(); err != nil {
				return p.failedStart(err)
			}
			return p, nil
		case <-p.peer.done:
			err = p.peer.connectionError()
		case <-handshake.Done():
			err = handshake.Err()
		}
		return p.failedStart(fmt.Errorf("initialize private worker: %w", err))
	} else {
		return p.failedStart(err)
	}
}

func (p *Process) failedStart(cause error) (*Process, error) {
	err := errors.Join(cause, p.Close())
	select {
	case <-p.done:
		return nil, err
	default:
		return p, err
	}
}

func (p *Process) PID() int              { return p.cmd.Process.Pid }
func (p *Process) RunID() string         { return p.peer.RunID() }
func (p *Process) Epoch() string         { return p.peer.Epoch() }
func (p *Process) Done() <-chan struct{} { return p.done }
func (p *Process) Call(ctx context.Context, method string, in, out any) error {
	return p.peer.Call(ctx, method, in, out)
}
func (p *Process) Wait() error {
	<-p.done
	return p.waitErr
}

// Close succeeds only after OS Wait and this connection's I/O, dispatch workers
// and pending Calls have all finished. A non-nil result must not be mistaken for
// complete reclamation. Done stays open until both boundaries are confirmed.
func (p *Process) Close() error {
	return p.CloseContext(context.Background())
}

func (p *Process) CloseContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("private worker CloseContext requires a context")
	}
	p.startStop()
	select {
	case <-p.done:
		return p.closeResult()
	default:
	}
	select {
	case <-p.done:
		return p.closeResult()
	case <-p.stopped:
		select {
		case <-p.done:
			return p.closeResult()
		default:
		}
		return p.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Process) closeResult() error {
	p.signalMu.Lock()
	defer p.signalMu.Unlock()
	if p.waitErr == nil || expectedTermination(p.waitErr, p.termSent.Load(), p.killSent.Load()) {
		return nil
	}
	return p.waitErr
}

func (p *Process) startStop() {
	p.stopOne.Do(func() {
		go func() {
			defer close(p.stopped)
			p.peer.shutdown(ErrClosed)
			select {
			case <-p.done:
				return
			default:
			}
			// Give EOF-driven normal exit (including race-runtime exit status)
			// time to finish before using signals that could mask that status.
			timer := time.NewTimer(p.grace)
			select {
			case <-p.done:
				timer.Stop()
				return
			case <-timer.C:
			}
			select {
			case <-p.osDone:
			default:
				p.signalMu.Lock()
				if terminateProcess(p.cmd) == nil {
					p.termSent.Store(true)
				}
				p.signalMu.Unlock()
			}
			timer.Reset(p.grace)
			select {
			case <-p.done:
				timer.Stop()
				return
			case <-timer.C:
			}
			select {
			case <-p.osDone:
			default:
				p.signalMu.Lock()
				if killProcess(p.cmd) == nil {
					p.killSent.Store(true)
				}
				p.signalMu.Unlock()
			}
			timer.Reset(p.killWait)
			defer timer.Stop()
			select {
			case <-p.done:
			case <-timer.C:
				select {
				case <-p.osDone:
					p.stopErr = errors.Join(ErrNotQuiescent, p.waitErr)
				default:
					p.stopErr = ErrNotReaped
				}
			}
		}()
	})
}

func replacePrivateSetting(env []string, key, value string) []string {
	result := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, key+"=") {
			result = append(result, entry)
		}
	}
	return append(result, key+"="+value)
}

func privateEnvironment(base []string) []string {
	if base == nil {
		base = os.Environ()
	}
	allowed := map[string]bool{"PATH": true, "LANG": true, "LC_ALL": true, "LC_CTYPE": true, "TZ": true, "TMPDIR": true, "GOMAXPROCS": true, "GOMEMLIMIT": true, "GOGC": true, "GOTRACEBACK": true, "GODEBUG": true, "GORACE": true}
	result := make([]string, 0, len(allowed)+1)
	for _, item := range base {
		key, _, ok := strings.Cut(item, "=")
		if ok && allowed[key] {
			result = append(result, item)
		}
	}
	return append(result, privateMarker+"=1")
}

func Serve(ctx context.Context, kind string, factory Factory) error {
	// Check the marker before touching arbitrary operator-owned FD 3 or 4.
	if os.Getenv(privateMarker) != "1" || factory == nil || ctx == nil {
		return ErrPrivateRole
	}
	if err := preparePrivatePipes(); err != nil {
		return fmt.Errorf("prepare private worker pipes: %w", err)
	}
	r, w := os.NewFile(3, "private-worker-in"), os.NewFile(4, "private-worker-out")
	if r == nil || w == nil {
		return ErrPrivateRole
	}
	defer r.Close()
	defer w.Close()
	for _, f := range []*os.File{r, w} {
		info, err := f.Stat()
		if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
			return ErrPrivateRole
		}
	}
	if err := privateFilesCloseOnExec(r, w); err != nil {
		return fmt.Errorf("protect private worker pipes: %w", err)
	}
	// A malformed/manual invocation cannot hold the private role indefinitely.
	// Closing inherited pipes also makes an idle handshake cancellable.
	handshake, cancelHandshake := context.WithTimeout(ctx, 10*time.Second)
	stop := context.AfterFunc(handshake, func() { r.Close(); w.Close() })
	hello, err := readFrame(r)
	stopped := stop()
	cancelHandshake()
	if !stopped && handshake.Err() != nil {
		return handshake.Err()
	}
	if err != nil {
		return err
	}
	_, epochErr := hex.DecodeString(hello.Epoch)
	if hello.Type != "hello" || hello.Version != ProtocolVersion || hello.Kind != kind || hello.RunID == "" || len(hello.RunID) > 256 || strings.ContainsAny(hello.RunID, "\r\n\x00") || len(hello.Epoch) != 48 || epochErr != nil || hello.Build == "" || hello.Build != model.BuildDetails().BinaryHash {
		return fmt.Errorf("%w: private worker handshake mismatch", ErrProtocol)
	}
	peer := newPeer(ctx, hello.identity, r, w, nil)
	defer peer.shutdown(ErrClosed)
	type initialized struct {
		handler Handler
		err     error
	}
	ready := make(chan initialized, 1)
	go func() {
		handler, err := factory(peer.ctx, kind, hello.Body, peer)
		ready <- initialized{handler, err}
	}()
	select {
	case init := <-ready:
		if init.err != nil || init.handler == nil {
			// Closing the supervisor pipes cancels bootstrap callbacks too. The
			// callback result and peer.done can become ready together; selecting
			// this branch must preserve the same connection-close classification.
			if init.err != nil && peer.ctx.Err() != nil && normalClose(init.err) == nil {
				return normalClose(peer.connectionError())
			}
			if init.err == nil {
				init.err = errors.New("private worker factory returned no handler")
			}
			notice, cancel := context.WithTimeout(peer.ctx, time.Second)
			peer.send(notice, frame{identity: peer.id, Type: "abort", Error: remoteFailure(init.err)}, true)
			cancel()
			return init.err
		}
		peer.mu.Lock()
		peer.handler = init.handler
		peer.mu.Unlock()
		if err := peer.send(peer.ctx, frame{identity: peer.id, Type: "ready"}, true); err != nil {
			if normalClose(err) == nil {
				return normalClose(peer.connectionError())
			}
			return err
		}
	case <-peer.done:
		return normalClose(peer.connectionError())
	}
	<-peer.done
	return normalClose(peer.connectionError())
}

func normalClose(err error) error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return err
		}
		for _, cause := range causes {
			if normalClose(cause) != nil {
				return err
			}
		}
		return nil
	}
	if cause := errors.Unwrap(err); cause != nil {
		if normalClose(cause) == nil {
			return nil
		}
		return err
	}
	// The peer may notice either pipe first during an ordinary supervisor
	// shutdown. A callback still writing sees EPIPE where the reader sees EOF.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE) || errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) || errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}
