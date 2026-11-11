package master

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// restartRepublish intentionally uses remove semantics, retaining persistent
// volumes and the Peer overlay, before publishing and deploying the new image.
func (s *swarm) restartRepublish(ctx context.Context) error {
	if err := s.a.Preflight(ctx, s.config, true); err != nil {
		return err
	}
	if err := s.checkAnalysisSwarm(ctx); err != nil {
		return err
	}
	if s.config["GRAFANA_ADMIN_PASSWORD"] == "" {
		return errors.New("GRAFANA_ADMIN_PASSWORD is required")
	}
	if err := s.checkPublishContext(); err != nil {
		return err
	}
	host, err := s.controlHost(ctx)
	if err != nil {
		return err
	}
	metrics := "http://" + host + ":" + s.config["KPL_HTTP_PORT"] + "/metrics"
	prometheus := "http://" + host + ":" + s.config["PROMETHEUS_PORT"] + "/"
	s.a = s.a.WithEnv(map[string]*string{"KPL_CONTROLLER_METRICS_URL": &metrics, "KPL_PROMETHEUS_EXTERNAL_URL": &prometheus})
	compose, cleanup, err := s.deploymentComposeFiles()
	if err != nil {
		return err
	}
	defer cleanup()
	// Validate the same deployment assets before removal, without printing
	// the Compose output that contains interpolated login credentials.
	quiet := *s.a
	quiet.Stdout = io.Discard
	checkCtx, cancel := context.WithTimeout(ctx, s.timeout)
	err = quiet.Run(checkCtx, "docker", append([]string{"stack", "config"}, compose...)...)
	cancel()
	if err != nil {
		return err
	}
	control := s.config["KPL_CONTROL_NODE_ID"]
	if err := s.readyNode(ctx, control); err != nil {
		return err
	}
	role, err := s.inspectNode(ctx, "{{.Spec.Role}}", control)
	if err != nil {
		return err
	}
	if role != "manager" {
		return errors.New("control node must be a Swarm manager")
	}

	// remove clears placement labels. Freeze the requested selection first;
	// a bare invocation must restore its previously labelled Agent hosts.
	var nodes []string
	if s.options.selector == "" && len(s.options.nodes) == 0 {
		nodes, err = s.selectedNodes(ctx)
	} else {
		nodes, err = s.commandNodes(ctx)
	}
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		if seen[node] {
			return errors.New("duplicate Agent node in placement inventory; stack was not removed")
		}
		seen[node] = true
		if err := s.readyNode(ctx, node); err != nil {
			return err
		}
	}
	minimum, err := positiveSwarmInteger(s.config["KPL_MIN_AGENTS"])
	if err != nil {
		return err
	}
	if int64(len(nodes)) < minimum {
		return fmt.Errorf("republish requires at least %d eligible Agent nodes; specify nodes or a selector", minimum)
	}
	if workers := s.analysisWorkers(); len(workers) != 0 {
		// Republish reuses worker service names. Confirm the old tasks have
		// stopped before creating new tasks at the same analysis endpoints.
		for _, worker := range workers {
			if _, err := s.captureTasks(ctx, worker.id, ""); err != nil {
				return err
			}
		}
		if s.present["controller"] {
			if err := s.stopService(ctx, s.stack+"_controller", s.controllerTimeout); err != nil {
				return err
			}
		}
		for _, worker := range workers {
			if err := s.stopService(ctx, worker.id, s.agentTimeout); err != nil {
				return err
			}
		}
	}
	s.reportNodes(nodes)
	fmt.Fprintln(s.a.Stdout, "Republish 1/3: removing stack services; persistent data volumes and the Peer network are retained.")
	if err := s.remove(ctx); err != nil {
		return fmt.Errorf("republish stopped during removal; image publishing and deployment were not started: %w", err)
	}
	fmt.Fprintln(s.a.Stdout, "Republish 2/3: building and publishing the configured image tag.")
	if err := s.publish(ctx); err != nil {
		return fmt.Errorf("republish failed after stack removal; the stack was not restored: %w", err)
	}
	// Deploy must inspect fresh service ownership and use the initial-deployment
	// path; the ordinary restart path requires the services we just removed.
	s.options.name, s.options.selector, s.options.nodes = "deploy", "", nodes
	if err := s.loadServices(ctx); err != nil {
		return fmt.Errorf("republish failed after stack removal; the stack was not restored: %w", err)
	}
	fmt.Fprintln(s.a.Stdout, "Republish 3/3: resolving the published tag and deploying its current registry digest.")
	if err := s.deploy(ctx); err != nil {
		return fmt.Errorf("republish deployment failed after stack removal; the stack was not restored: %w", err)
	}
	return nil
}
