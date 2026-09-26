package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const (
	peerContainerAPIPort   = 18000
	peerContainerP2PPort   = 20000
	peerContainerConfig    = "/etc/kpl-peer.json"
	managedContainerLabel  = "io.kpl.managed=true"
	dockerAdmissionTimeout = 45 * time.Second
	dockerStartupTimeout   = 45 * time.Second
	dockerReconcileWorkers = 4
)

var (
	errDockerCleanup  = errors.New("Docker cleanup failed")
	dockerNetworkName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
	dockerContainerID = regexp.MustCompile(`^[a-f0-9]{64}$`)
	dockerPeerName    = regexp.MustCompile(`^kpl-peer-[a-f0-9]{20}-[a-f0-9]{12}$`)
)

// dockerRuntime runs one peer in each private container network namespace. It
// never publishes host ports, shares the host network, or mounts the Docker
// socket (or another host path) into a peer container.
type dockerRuntime struct {
	binary         string
	image          string
	network        string
	commandContext func(context.Context, string, ...string) *exec.Cmd
}

func (d *dockerRuntime) check(ctx context.Context) error {
	if !dockerNetworkName.MatchString(d.network) || d.network == "host" || d.network == "none" || d.network == "bridge" {
		return fmt.Errorf("docker peer network must be a Swarm attachable overlay network")
	}
	if d.image == "" || strings.HasPrefix(d.image, "-") {
		return fmt.Errorf("docker peer image is required")
	}
	infoFormat := `{"OSType":{{json .OSType}},"SwarmState":{{json .Swarm.LocalNodeState}}}`
	output, err := d.run(ctx, nil, "info", "--format", infoFormat)
	if err != nil {
		return fmt.Errorf("connect to Docker daemon: %w", err)
	}
	var daemon struct {
		OSType     string
		SwarmState string
	}
	if err := json.Unmarshal(output, &daemon); err != nil {
		return fmt.Errorf("invalid Docker daemon info response: %w", err)
	}
	if daemon.OSType != "linux" {
		return fmt.Errorf("Docker peer runtime requires a Linux Docker daemon")
	}
	if daemon.SwarmState != "active" {
		return fmt.Errorf("Docker peer runtime requires an active Swarm node")
	}
	output, err = d.run(ctx, nil, "image", "inspect", "--format", "{{.Os}}", d.image)
	if err != nil {
		return fmt.Errorf("inspect peer image %q (build or pull it first): %w", d.image, err)
	}
	if strings.TrimSpace(string(output)) != "linux" {
		return fmt.Errorf("Docker peer image %q must be a Linux image", d.image)
	}
	// Full network inspection includes every attached container. Select only
	// startup requirements so large experiments fit the diagnostic output limit.
	networkFormat := `[{"Name":{{json .Name}},"Driver":{{json .Driver}},"Attachable":{{json .Attachable}},"Scope":{{json .Scope}}}]`
	output, err = d.run(ctx, nil, "network", "inspect", "--format", networkFormat, d.network)
	if err != nil {
		return fmt.Errorf("inspect peer network %q (create it first): %w", d.network, err)
	}
	var networks []struct {
		Name       string
		Driver     string
		Attachable bool
		Scope      string
	}
	if err := json.Unmarshal(output, &networks); err != nil || len(networks) != 1 {
		return fmt.Errorf("invalid Docker network inspect response for %q", d.network)
	}
	network := networks[0]
	if !dockerNetworkName.MatchString(network.Name) || network.Driver != "overlay" || network.Scope != "swarm" {
		return fmt.Errorf("peer network %q must use a Swarm overlay driver", d.network)
	}
	if !network.Attachable {
		return fmt.Errorf("peer overlay network %q must be attachable", d.network)
	}
	// Docker accepts a network ID as well as a name, but container inspection
	// keys NetworkSettings.Networks by canonical name. Use that same identity
	// for creation, address lookup and reconciliation labels.
	d.network = network.Name
	return nil
}

func (d *dockerRuntime) create(ctx context.Context, node model.Node, configData []byte) (id, apiURL string, err error) {
	return d.createWithAdmission(ctx, node, configData, nil)
}

func (d *dockerRuntime) createWithAdmission(ctx context.Context, node model.Node, configData []byte, admitted func()) (id, apiURL string, err error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	var config model.PeerProcessConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		return "", "", fmt.Errorf("decode peer container configuration: %w", err)
	}
	name, err := peerContainerName(node)
	if err != nil {
		return "", "", err
	}
	args := []string{"create", "--name", name,
		"--label", managedContainerLabel,
		"--label", "io.kpl.agent=" + node.AgentID,
		"--label", "io.kpl.run=" + node.RunID,
		"--label", "io.kpl.node=" + node.ID,
		"--label", "io.kpl.generation=" + strconv.FormatUint(node.Generation, 10),
		"--label", "io.kpl.network=" + d.network,
		"--network", d.network, "--user", "0:0", "--cap-drop", "ALL",
		"--security-opt", "no-new-privileges", "--init",
		"--log-driver", "json-file", "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
	}
	if config.NodeConfig.Network.Enabled() {
		args = append(args, "--cap-add", "NET_ADMIN")
	}
	args = append(args, d.image, "peer", "--config", peerContainerConfig)
	// A unique attempt suffix lets cleanup find a container even if the CLI is
	// interrupted after daemon creation but before it returns the container ID.
	cleanupTarget := name
	defer func() {
		if err == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if cleanupErr := d.remove(cleanupCtx, cleanupTarget); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("%w for peer container %s: %w", errDockerCleanup, cleanupTarget, cleanupErr))
			// Preserve the exact attempt ID/name so stop-all can retry a
			// transient cleanup failure without touching unrelated peers.
			id, apiURL = cleanupTarget, ""
			return
		}
		id, apiURL = "", ""
	}()
	// A canceled Docker create HTTP request can still commit on the daemon.
	// Wait for its admission result so cleanup has a real ID rather than racing
	// a not-yet-created name. Admission has its own bounded budget and still
	// honors an earlier caller deadline.
	deadline := time.Now().Add(dockerAdmissionTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	admissionCtx, admissionCancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	output, err := d.run(admissionCtx, nil, args...)
	admissionCancel()
	if err != nil {
		return "", "", fmt.Errorf("create peer container: %w", err)
	}
	id = strings.TrimSpace(string(output))
	if !dockerContainerID.MatchString(id) {
		return "", "", fmt.Errorf("Docker create returned an invalid container ID")
	}
	cleanupTarget = id
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if admitted != nil {
		admitted()
	}
	// Busy daemons can spend much of admission's budget just allocating the
	// container. Give copying, startup and inspection a fresh shared budget;
	// unlike admission, every startup step is immediately cancelable.
	startupCtx, startupCancel := context.WithTimeout(ctx, dockerStartupTimeout)
	defer startupCancel()
	archive, err := peerConfigArchive(configData)
	if err != nil {
		return "", "", err
	}
	// docker cp accepts a tar stream and works before container startup, which
	// also supports a remote daemon without sharing the Agent's filesystem.
	if _, err := d.run(startupCtx, bytes.NewReader(archive), "cp", "-", id+":/"); err != nil {
		return "", "", fmt.Errorf("copy peer container configuration: %w", err)
	}
	if _, err := d.run(startupCtx, nil, "start", id); err != nil {
		return "", "", fmt.Errorf("start peer container: %w", err)
	}
	output, err = d.run(startupCtx, nil, "inspect", "--format", "{{json .NetworkSettings.Networks}}", id)
	if err != nil {
		return "", "", fmt.Errorf("inspect peer container address: %w", err)
	}
	var networks map[string]struct{ IPAddress string }
	if err := json.Unmarshal(output, &networks); err != nil {
		return "", "", fmt.Errorf("decode peer container address: %w", err)
	}
	ip := net.ParseIP(networks[d.network].IPAddress)
	if ip == nil || ip.To4() == nil || ip.IsLoopback() || ip.IsUnspecified() {
		// A peer that fails during netem/bootstrap setup can exit before this
		// inspect, clearing its endpoint. Preserve its startup error before
		// deferred cleanup removes the container and its logs.
		logCtx, logCancel := context.WithTimeout(startupCtx, 5*time.Second)
		defer logCancel()
		logs, logErr := d.run(logCtx, nil, "logs", "--tail", "30", id)
		if logErr == nil && len(bytes.TrimSpace(logs)) != 0 {
			return "", "", fmt.Errorf("peer container has no usable IPv4 address on network %q; startup logs: %s", d.network, bytes.TrimSpace(logs))
		}
		return "", "", fmt.Errorf("peer container has no usable IPv4 address on network %q", d.network)
	}
	if err := startupCtx.Err(); err != nil {
		return "", "", err
	}
	return id, "http://" + net.JoinHostPort(ip.String(), strconv.Itoa(peerContainerAPIPort)), nil
}

func (d *dockerRuntime) wait(ctx context.Context, id string) error {
	if !dockerContainerID.MatchString(id) {
		return fmt.Errorf("invalid peer container ID")
	}
	output, err := d.run(ctx, nil, "wait", id)
	if err != nil {
		return fmt.Errorf("wait for peer container: %w", err)
	}
	code, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return fmt.Errorf("Docker wait returned an invalid exit code")
	}
	if code != 0 {
		logCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		logs, logErr := d.run(logCtx, nil, "logs", "--tail", "30", id)
		if logErr == nil && len(bytes.TrimSpace(logs)) != 0 {
			return fmt.Errorf("peer container exited with code %d: %s", code, bytes.TrimSpace(logs))
		}
		return fmt.Errorf("peer container exited with code %d", code)
	}
	return nil
}

func (d *dockerRuntime) remove(ctx context.Context, id string) error {
	if !dockerContainerID.MatchString(id) && !dockerPeerName.MatchString(id) {
		return fmt.Errorf("refusing to remove invalid peer container ID or name")
	}
	output, err := d.run(ctx, nil, "rm", "--force", id)
	if err == nil || strings.Contains(strings.ToLower(string(output)), "no such container:") {
		return nil
	}
	message := strings.ToLower(string(output))
	if strings.Contains(message, "removal of container") && strings.Contains(message, "is already in progress") {
		return d.waitUntilRemoved(ctx, id)
	}
	return fmt.Errorf("remove peer container %s: %w", id, err)
}

// Docker may keep removing a container after its original CLI command was
// canceled. A second rm then reports a conflict, not completion. Only an
// explicit not-found response confirms cleanup; daemon failures are preserved.
func (d *dockerRuntime) waitUntilRemoved(ctx context.Context, id string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		output, err := d.run(ctx, nil, "inspect", "--type", "container", "--format", "{{.Id}}", id)
		if err != nil {
			message := strings.ToLower(string(output))
			if strings.Contains(message, "no such container:") || strings.Contains(message, "no such object:") {
				return nil
			}
			return fmt.Errorf("wait for peer container %s removal: %w", id, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for peer container %s removal: %w", id, ctx.Err())
		case <-ticker.C:
		}
	}
}

// removeAgentContainers reclaims peers left behind by an Agent crash. Agent IDs
// must be unique for all Agents using the same Docker daemon and peer network.
func (d *dockerRuntime) removeAgentContainers(ctx context.Context, agentID string) error {
	ids, err := d.agentContainerIDs(ctx, agentID)
	if err != nil {
		return err
	}
	// Serial removal makes startup proportional to every leftover container's
	// Docker latency. Keep concurrency small to avoid flooding the local daemon,
	// while allowing other leftovers to be reclaimed if one removal is slow.
	failures := make([]error, len(ids))
	jobs := make(chan int, len(ids))
	for i := range ids {
		jobs <- i
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(dockerReconcileWorkers, len(ids)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					return
				}
				failures[i] = d.remove(ctx, ids[i])
			}
		}()
	}
	workers.Wait()
	return errors.Join(ctx.Err(), errors.Join(failures...))
}

func (d *dockerRuntime) agentContainerIDs(ctx context.Context, agentID string) ([]string, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required for Docker reconciliation")
	}
	// A large experiment can leave more IDs than the diagnostic output limit.
	// Collect the complete scoped inventory so a restart reclaims every peer.
	output, err := d.runWithOutput(ctx, nil, &bytes.Buffer{}, "ps", "--all", "--quiet", "--no-trunc",
		"--filter", "label="+managedContainerLabel, "--filter", "label=io.kpl.agent="+agentID,
		"--filter", "label=io.kpl.network="+d.network)
	if err != nil {
		return nil, fmt.Errorf("list previous Agent containers: %w", err)
	}
	ids := strings.Fields(string(output))
	for _, id := range ids {
		if !dockerContainerID.MatchString(id) {
			return nil, fmt.Errorf("Docker container inventory returned an invalid container ID")
		}
	}
	return ids, nil
}

func peerContainerName(node model.Node) (string, error) {
	identity, err := json.Marshal([]any{node.AgentID, node.RunID, node.ID, node.Generation})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(identity)
	var attempt [6]byte
	if _, err := rand.Read(attempt[:]); err != nil {
		return "", fmt.Errorf("generate peer container name: %w", err)
	}
	return fmt.Sprintf("kpl-peer-%x-%x", digest[:10], attempt), nil
}

func peerConfigArchive(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	w := tar.NewWriter(&buffer)
	if err := w.WriteHeader(&tar.Header{Name: strings.TrimPrefix(peerContainerConfig, "/"), Mode: 0600, Size: int64(len(data))}); err != nil {
		return nil, fmt.Errorf("prepare peer config archive: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("write peer config archive: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close peer config archive: %w", err)
	}
	return buffer.Bytes(), nil
}

// Diagnostic CLI output is bounded so a noisy Docker daemon or peer cannot grow
// the Agent indefinitely. Keep the buffer private: embedding it exposes
// io.ReaderFrom, allowing io.Copy in os/exec to bypass the Write limit.
type limitedDockerOutput struct{ buffer bytes.Buffer }

func (b *limitedDockerOutput) Bytes() []byte { return b.buffer.Bytes() }

func (b *limitedDockerOutput) Write(data []byte) (int, error) {
	n := len(data)
	if remaining := 32*1024 - b.buffer.Len(); remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		_, _ = b.buffer.Write(data)
	}
	return n, nil
}

func (d *dockerRuntime) run(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
	return d.runWithOutput(ctx, input, &limitedDockerOutput{}, args...)
}

func (d *dockerRuntime) runWithOutput(ctx context.Context, input io.Reader, output interface {
	io.Writer
	Bytes() []byte
}, args ...string) ([]byte, error) {
	commandContext := d.commandContext
	if commandContext == nil {
		commandContext = exec.CommandContext
	}
	command := commandContext(ctx, d.binary, args...)
	command.Stdin = input
	var stderr limitedDockerOutput
	command.Stdout = output
	command.Stderr = &stderr
	err := command.Run()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		combined := append(output.Bytes(), stderr.Bytes()...)
		return combined, fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(string(combined)))
	}
	// Docker logs forwards the container's stderr there even on success. For
	// machine-readable commands, keep CLI warnings separate from stdout JSON/IDs.
	if args[0] == "logs" {
		return append(output.Bytes(), stderr.Bytes()...), nil
	}
	return output.Bytes(), nil
}
