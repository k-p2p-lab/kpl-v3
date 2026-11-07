package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var (
	hostSwarmNodeID = regexp.MustCompile(`^[a-z0-9]{25}$`)
	hostBootUUID    = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)
	hostOperationID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	hostImageID     = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

const hostRebootTimeout = 45 * time.Second

// All helper programs and arguments are fixed by the application. The only
// substituted shell input is a validated UUID passed as a positional argument.
const hostSystemdPreflight = `test "$(cat /proc/1/comm)" = systemd && test -d /run/systemd/system && command -v systemctl >/dev/null && cat /proc/sys/kernel/random/boot_id`
const hostSystemdReboot = `test "$(cat /proc/sys/kernel/random/boot_id)" = "$1" && test "$(cat /proc/1/comm)" = systemd && test -d /run/systemd/system && exec systemctl --system --no-ask-password reboot`

func readHostBootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(data))
	if !hostBootUUID.MatchString(id) {
		return "", errors.New("invalid Linux host boot ID")
	}
	return id, nil
}

func validHostRebootRequest(request model.HostRebootRequest) bool {
	return hostOperationID.MatchString(request.OperationID) && hostSwarmNodeID.MatchString(request.NodeID) &&
		hostBootUUID.MatchString(request.BootID) && !request.AgentStartedAt.IsZero()
}

func (s *Server) initializeHostReboot() error {
	if s.hostBootIDReader == nil {
		s.hostBootIDReader = readHostBootID
	}
	bootID, bootErr := s.hostBootIDReader()
	s.host = &model.AgentHost{NodeID: s.config.Labels["swarmNodeId"], BootID: bootID, RebootEnabled: s.config.HostRebootEnabled, RebootReason: "Host reboot is disabled on this Agent."}
	if s.config.HostRebootEnabled {
		s.host.RebootReason = "Host reboot capability has not been checked."
	}
	receipt, err := s.readHostRebootReceipt()
	if err != nil {
		return err
	}
	if receipt != nil && bootErr == nil && receipt.Request.BootID == bootID && receipt.Result.State == "unknown" {
		rejection, err := s.readHostRebootRejection(receipt.Request)
		if err != nil {
			return fmt.Errorf("verify retained reboot rejection: %w", err)
		}
		if rejection != nil {
			// Preparing the main receipt can fail after rename but before its
			// directory sync. A durable rejection confirms no helper was started.
			receipt = nil
		}
	}
	s.hostRebootReceipt = receipt
	s.telemetryHostRebootClosed = false
	unknownBoot := bootErr != nil || !hostBootUUID.MatchString(bootID)
	if receipt != nil && (unknownBoot || receipt.Request.BootID == bootID) {
		s.host.RebootPending = true
		s.host.RebootReason = "A reboot was already requested on this boot; automatic retry is disabled."
		s.telemetryHostRebootClosed = true
	}
	if receipt != nil && unknownBoot {
		if bootErr == nil {
			bootErr = errors.New("invalid Linux host boot ID")
		}
		return fmt.Errorf("cannot establish host boot identity while a reboot receipt is retained: %w", bootErr)
	}
	return nil
}

// Caller holds mu; no published snapshot shares the mutable capability record.
func (s *Server) hostStatusLocked() *model.AgentHost {
	if s.host == nil {
		return nil
	}
	host := *s.host
	return &host
}

// Explicit --host overrides DOCKER_HOST and context selection. A successful
// preflight must additionally prove that daemon PID 1 has this Agent's boot ID.
func (s *Server) runHostDocker(ctx context.Context, args ...string) ([]byte, error) {
	if s.hostDockerCommand != nil {
		return s.hostDockerCommand(ctx, args...)
	}
	socket := s.config.DockerSocket
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	if !filepath.IsAbs(socket) || strings.ContainsAny(socket, "\x00\n\r") {
		return nil, errors.New("host reboot requires an absolute local Docker socket")
	}
	return s.docker.run(ctx, nil, append([]string{"--host", "unix://" + socket}, args...)...)
}

type rebootHostEnvironment struct {
	host  model.AgentHost
	image string
}

func (s *Server) inspectRebootHost(ctx context.Context) (rebootHostEnvironment, error) {
	environment := rebootHostEnvironment{host: model.AgentHost{NodeID: s.config.Labels["swarmNodeId"], RebootEnabled: s.config.HostRebootEnabled}}
	if !s.config.HostRebootEnabled {
		return environment, errors.New("Host reboot is disabled on this Agent.")
	}
	if s.config.Token == "" || s.config.HostRebootToken == "" || auth.Equal(s.config.Token, s.config.HostRebootToken) || !hostSwarmNodeID.MatchString(environment.host.NodeID) || !hostSwarmNodeID.MatchString(s.config.ControlNodeID) || s.config.SelfContainer == "" || strings.HasPrefix(s.config.SelfContainer, "-") {
		return environment, errors.New("Host reboot requires internal authentication and trusted Swarm task/control-node identities.")
	}
	bootID, err := s.hostBootIDReader()
	if err != nil {
		return environment, fmt.Errorf("read host boot identity: %w", err)
	}
	environment.host.BootID = bootID
	infoFormat := `{"OSType":{{json .OSType}},"Name":{{json .Name}},"SecurityOptions":{{json .SecurityOptions}},"Swarm":{{json .Swarm}}}`
	output, err := s.runHostDocker(ctx, "info", "--format", infoFormat)
	if err != nil {
		return environment, fmt.Errorf("inspect reboot host: %w", err)
	}
	var daemon struct {
		OSType, Name    string
		SecurityOptions []string
		Swarm           struct {
			NodeID, LocalNodeState string
			ControlAvailable       *bool
		}
	}
	if json.Unmarshal(output, &daemon) != nil || daemon.OSType != "linux" || daemon.Swarm.LocalNodeState != "active" || daemon.Swarm.NodeID != environment.host.NodeID || daemon.Swarm.ControlAvailable == nil || daemon.SecurityOptions == nil {
		return environment, errors.New("Host reboot requires the Agent's matching active Linux Swarm node.")
	}
	environment.host.Hostname = daemon.Name
	if *daemon.Swarm.ControlAvailable || daemon.Swarm.NodeID == s.config.ControlNodeID {
		return environment, errors.New("Manager and Controller hosts cannot be rebooted from the dashboard.")
	}
	for _, option := range daemon.SecurityOptions {
		lower := strings.ToLower(option)
		if strings.Contains(lower, "rootless") || strings.Contains(lower, "userns") {
			return environment, errors.New("Host reboot requires rootful Docker without user namespace remapping.")
		}
	}
	selfFormat := `{"Image":{{json .Image}},"NodeID":{{json (index .Config.Labels "com.docker.swarm.node.id")}},"Running":{{json .State.Running}}}`
	output, err = s.runHostDocker(ctx, "inspect", "--type", "container", "--format", selfFormat, s.config.SelfContainer)
	if err != nil {
		return environment, fmt.Errorf("inspect Agent helper image: %w", err)
	}
	var self struct {
		Image, NodeID string
		Running       bool
	}
	if json.Unmarshal(output, &self) != nil || !self.Running || self.NodeID != daemon.Swarm.NodeID || !hostImageID.MatchString(self.Image) {
		return environment, errors.New("Cannot verify the running Agent's immutable local helper image.")
	}
	environment.image = self.Image
	output, err = s.runHostDocker(ctx, append(helperDockerArguments("run", environment.image), "--", "/bin/sh", "-c", hostSystemdPreflight)...)
	if err != nil || strings.TrimSpace(string(output)) != bootID {
		return environment, errors.New("Host reboot requires a local systemd host with a matching boot identity and nsenter support.")
	}
	environment.host.RebootSupported = true
	return environment, nil
}

func helperDockerArguments(command, image string) []string {
	args := []string{command}
	if command == "run" {
		args = append(args, "--rm")
	}
	return append(args, "--pull=never", "--network=none", "--pid=host", "--privileged", "--user=0:0", "--restart=no", "--entrypoint=/usr/bin/nsenter", image,
		"--target=1", "--mount", "--pid", "--root", "--wd")
}

func (s *Server) refreshHostRebootCapability(parent context.Context) {
	s.hostRebootMu.Lock()
	defer s.hostRebootMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, hostRebootTimeout)
	defer cancel()
	environment, err := s.inspectRebootHost(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	environment.host.RebootPending = s.host != nil && s.host.RebootPending
	if err != nil {
		environment.host.RebootReason = fmt.Sprintf("%.1024s", err.Error())
	}
	if environment.host.RebootPending {
		environment.host.RebootReason = "A reboot was already requested on this boot; automatic retry is disabled."
	}
	s.host = &environment.host
}

func (s *Server) handleHostReboot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	// This endpoint remains authenticated even in an otherwise unauthenticated
	// development Agent. No browser-supplied host command is accepted.
	if s.config.Token == "" || s.config.HostRebootToken == "" || auth.Equal(s.config.Token, s.config.HostRebootToken) || r.Header.Get("Authorization") != "Bearer "+s.config.Token || r.Header.Get(model.HostRebootAuthHeader) == "" {
		writeError(w, http.StatusUnauthorized, "valid internal bearer and distinct host-control authentication are required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var request model.HostRebootRequest
	if err := decodeJSON(w, r, &request); err != nil || !validHostRebootRequest(request) {
		writeError(w, http.StatusBadRequest, "valid operationId, nodeId, bootId and agentStartedAt are required")
		return
	}
	request.AgentStartedAt = request.AgentStartedAt.UTC()
	canonical, err := json.Marshal(request)
	if err != nil || !auth.Equal(auth.HostRebootSignature(s.config.HostRebootToken, canonical), r.Header.Get(model.HostRebootAuthHeader)) {
		writeError(w, http.StatusUnauthorized, "valid host-control request signature is required")
		return
	}
	matched, err := s.hostRebootActorMatches(request)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if !matched {
		writeError(w, http.StatusConflict, "host or Agent identity changed; no acknowledgment can be authenticated for this target")
		return
	}
	result, status := s.rebootHost(r.Context(), request)
	result.Signature = ""
	ack, err := json.Marshal(result)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "host reboot acknowledgment is unavailable")
		return
	}
	result.Signature = auth.HostRebootAckSignature(s.config.HostRebootToken, canonical, ack)
	writeJSON(w, status, result)
}

func (s *Server) rebootHost(ctx context.Context, request model.HostRebootRequest) (model.HostRebootResult, int) {
	s.hostRebootMu.Lock()
	defer s.hostRebootMu.Unlock()
	result := model.HostRebootResult{OperationID: request.OperationID, NodeID: request.NodeID, BootID: request.BootID, State: "rejected"}
	actorMatches, committed := false, false
	unknownRejection := func(message string) (model.HostRebootResult, int) {
		result.State, result.Error = "unknown", message
		committed = true
		s.mu.Lock()
		if s.host == nil {
			s.host = &model.AgentHost{}
		}
		s.host.RebootPending, s.host.RebootReason = true, message
		s.historyRevision++
		s.mu.Unlock()
		return result, http.StatusAccepted
	}
	reject := func(status int, err error) (model.HostRebootResult, int) {
		result.State = "rejected"
		result.Error = fmt.Sprintf("%.2048s", err.Error())
		if actorMatches {
			if err := s.persistHostRebootRejection(request, result, status); err != nil {
				return unknownRejection("The rejection could not be made durable; no additional helper was started and the operation remains unresolved.")
			}
		}
		return result, status
	}
	if !validHostRebootRequest(request) {
		return reject(http.StatusConflict, errors.New("Host reboot identity is invalid."))
	}
	bootID, err := s.hostBootIDReader()
	if err != nil || request.BootID != bootID || request.NodeID != s.config.Labels["swarmNodeId"] {
		return reject(http.StatusConflict, errors.New("Host identity or boot has changed; refresh before requesting reboot."))
	}
	actorMatches = request.AgentStartedAt.Equal(s.startedAt)
	if actorMatches {
		rejection, err := s.readHostRebootRejection(request)
		if err != nil {
			return unknownRejection("A past reboot rejection could not be verified; no additional helper was started.")
		}
		if rejection != nil {
			return rejection.Result, rejection.Status
		}
	}
	if !s.config.HostRebootEnabled {
		return reject(http.StatusConflict, errors.New("Host reboot is disabled on this Agent."))
	}
	if receipt := s.hostRebootReceipt; receipt != nil && receipt.Request.BootID == bootID {
		if receipt.Request.OperationID == request.OperationID && receipt.Request.NodeID == request.NodeID && receipt.Request.AgentStartedAt.Equal(request.AgentStartedAt) {
			return receipt.Result, http.StatusAccepted
		}
		return reject(http.StatusConflict, errors.New("A reboot operation is already recorded for this boot; automatic retry is disabled."))
	}
	if !request.AgentStartedAt.Equal(s.startedAt) {
		return reject(http.StatusConflict, errors.New("Agent instance changed; refresh before requesting reboot."))
	}
	s.mu.Lock()
	if !s.startupReconciled || s.shuttingDown || s.fencingAll || s.host != nil && s.host.RebootPending {
		s.mu.Unlock()
		return reject(http.StatusConflict, errors.New("Agent startup, shutdown or host maintenance is in progress."))
	}
	for _, process := range s.processes {
		if !processCleanupComplete(process) {
			s.mu.Unlock()
			return reject(http.StatusConflict, errors.New("Remove all Peers and confirm container cleanup before host reboot."))
		}
	}
	if s.host == nil {
		s.host = &model.AgentHost{}
	}
	s.host.RebootPending = true
	s.historyRevision++
	s.mu.Unlock()
	closedTelemetry := false
	defer func() {
		if committed {
			return
		}
		if closedTelemetry {
			s.eventsMu.Lock()
			s.telemetryHostRebootClosed = false
			s.eventsMu.Unlock()
		}
		s.mu.Lock()
		s.host.RebootPending = false
		s.historyRevision++
		s.mu.Unlock()
	}()
	s.eventsMu.Lock()
	if s.pendingEventsLocked() != 0 || s.spoolError != nil || s.telemetryClosed || s.telemetryHostRebootClosed || s.telemetryDecoders.Load() != 0 || s.telemetryWaiters.Load() != 0 {
		s.eventsMu.Unlock()
		return reject(http.StatusConflict, errors.New("Deliver all accepted telemetry and finish telemetry requests before host reboot."))
	}
	s.telemetryHostRebootClosed, closedTelemetry = true, true
	s.eventsMu.Unlock()
	preflightCtx, cancel := context.WithTimeout(ctx, hostRebootTimeout)
	defer cancel()
	environment, err := s.inspectRebootHost(preflightCtx)
	if err != nil {
		return reject(http.StatusConflict, err)
	}
	output, err := s.runHostDocker(preflightCtx, "ps", "--all", "--quiet", "--filter", "label="+managedContainerLabel)
	if err != nil || len(strings.TrimSpace(string(output))) != 0 {
		return reject(http.StatusConflict, errors.New("The Docker host still has KPL Peer containers or its inventory could not be verified."))
	}
	if err := preflightCtx.Err(); err != nil {
		return reject(http.StatusConflict, err)
	}
	s.mu.RLock()
	stopping := s.shuttingDown
	s.mu.RUnlock()
	s.eventsMu.Lock()
	telemetryStopped := s.telemetryClosed
	s.eventsMu.Unlock()
	if stopping || telemetryStopped {
		return reject(http.StatusConflict, errors.New("Agent shutdown began before reboot dispatch."))
	}
	result.State, result.Error = "unknown", "Reboot preparation was recorded; command acceptance is not yet confirmed."
	receipt := &hostRebootReceipt{Version: 1, Request: request, Result: result, UpdatedAt: time.Now().UTC()}
	if err := s.persistHostRebootReceipt(receipt); err != nil {
		return reject(http.StatusServiceUnavailable, fmt.Errorf("persist reboot operation before execution: %w", err))
	}
	s.hostRebootReceipt, committed = receipt, true
	// Once committed, client disconnection stops only HTTP waiting; this bounded
	// operation must not become an unrecorded partially started helper.
	executionCtx, executionCancel := context.WithTimeout(context.WithoutCancel(ctx), hostRebootTimeout)
	defer executionCancel()
	identity := sha256.Sum256([]byte(request.NodeID + ":" + request.BootID))
	name := "kpl-host-reboot-" + hex.EncodeToString(identity[:16])
	args := helperDockerArguments("create", environment.image)
	// Docker flags must precede the immutable image. Insert the fixed name and
	// identifying labels immediately after the create subcommand.
	args = append([]string{args[0], "--name", name, "--label", "io.kpl.host-reboot=true", "--label", "io.kpl.host-node=" + request.NodeID, "--label", "io.kpl.host-boot=" + request.BootID, "--label", "io.kpl.host-operation=" + request.OperationID}, args[1:]...)
	args = append(args, "--", "/bin/sh", "-c", hostSystemdReboot, "kpl-host-reboot", request.BootID)
	output, err = s.runHostDocker(executionCtx, args...)
	if err == nil && !dockerContainerID.MatchString(strings.TrimSpace(string(output))) {
		err = errors.New("Docker returned an unverified reboot helper identity")
	}
	if err == nil {
		_, err = s.runHostDocker(executionCtx, "start", "--attach", strings.TrimSpace(string(output)))
	}
	if err == nil {
		result.State, result.Error = "requested", ""
	} else {
		result.Error = "Reboot command outcome is unknown; automatic retry is disabled. " + fmt.Sprintf("%.2048s", err.Error())
	}
	receipt.Result, receipt.UpdatedAt = result, time.Now().UTC()
	if err := s.persistHostRebootReceipt(receipt); err != nil {
		result.State, result.Error = "unknown", "Reboot outcome could not be persisted; automatic retry is disabled."
		receipt.Result = result
		s.logger.Error("persist host reboot outcome", "operation", request.OperationID, "error", err)
	}
	s.mu.Lock()
	environment.host.RebootPending = true
	environment.host.RebootReason = "Host reboot was requested; waiting for a new boot and Agent registration."
	s.host = &environment.host
	s.mu.Unlock()
	return result, http.StatusAccepted
}
