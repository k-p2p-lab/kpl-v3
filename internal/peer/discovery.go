package peer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	corenetwork "github.com/libp2p/go-libp2p/core/network"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

const (
	discoveryPollInterval = 3 * time.Second
	discoveryRequestLimit = 5 * time.Second
	discoveryRetryDelay   = 15 * time.Second
)

type rankedDiscoveryPeer struct {
	peer  bootstrapNode
	score [sha256.Size]byte
}

type discoveryTopicPlan struct {
	topic      string
	needed     int
	candidates []bootstrapNode
	next       int
}

type discoveryDialResult struct {
	peerID corepeer.ID
	topics []string
	err    error
}

// selectDiscoveryPeers uses rendezvous hashing so registry response order does
// not disturb an established candidate set. Subscribers fill the set first;
// relays and publishers provide transport connectivity when there are fewer
// subscribers than the configured target.
func selectDiscoveryPeers(requesterNodeID, topic string, candidates []bootstrapNode, limit int) []bootstrapNode {
	if limit <= 0 {
		return nil
	}
	deduplicated := make(map[string]bootstrapNode, len(candidates))
	for _, candidate := range candidates {
		candidate.NodeID = strings.TrimSpace(candidate.NodeID)
		candidate.PeerID = strings.TrimSpace(candidate.PeerID)
		if candidate.NodeID == requesterNodeID || candidate.PeerID == "" {
			continue
		}
		if _, err := addrInfo(candidate); err != nil {
			continue
		}
		previous, exists := deduplicated[candidate.PeerID]
		if !exists || candidate.Subscribed && !previous.Subscribed ||
			candidate.Subscribed == previous.Subscribed && candidate.NodeID < previous.NodeID {
			deduplicated[candidate.PeerID] = candidate
		}
	}

	subscribers := make([]rankedDiscoveryPeer, 0, len(deduplicated))
	others := make([]rankedDiscoveryPeer, 0, len(deduplicated))
	for _, candidate := range deduplicated {
		ranked := rankedDiscoveryPeer{
			peer:  candidate,
			score: sha256.Sum256([]byte(requesterNodeID + "\x00" + topic + "\x00" + candidate.NodeID + "\x00" + candidate.PeerID)),
		}
		if candidate.Subscribed {
			subscribers = append(subscribers, ranked)
		} else {
			others = append(others, ranked)
		}
	}
	rank := func(peers []rankedDiscoveryPeer) {
		sort.Slice(peers, func(i, j int) bool {
			if comparison := bytes.Compare(peers[i].score[:], peers[j].score[:]); comparison != 0 {
				return comparison > 0
			}
			if peers[i].peer.NodeID != peers[j].peer.NodeID {
				return peers[i].peer.NodeID < peers[j].peer.NodeID
			}
			return peers[i].peer.PeerID < peers[j].peer.PeerID
		})
	}
	rank(subscribers)
	rank(others)

	selected := make([]bootstrapNode, 0, limit)
	for _, group := range [][]rankedDiscoveryPeer{subscribers, others} {
		for _, candidate := range group {
			if len(selected) == limit {
				return selected
			}
			selected = append(selected, candidate.peer)
		}
	}
	return selected
}

func (s *Server) fetchDiscoveryPeers(ctx context.Context, topic string) ([]bootstrapNode, error) {
	endpoint := strings.TrimRight(s.config.ControllerURL, "/") + "/api/v1/discovery"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	query := req.URL.Query()
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	query.Set("runId", s.config.Node.RunID)
	query.Set("topic", topic)
	query.Set("requesterNodeId", s.config.Node.ID)
	req.URL.RawQuery = query.Encode()
	resp, err := (&http.Client{Timeout: discoveryRequestLimit}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, fmt.Errorf("topic discovery registry returned %s", resp.Status)
	}
	var nodes []bootstrapNode
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (s *Server) discoveryLoop(ctx context.Context) {
	s.connectDiscoveryRound(ctx)
	ticker := time.NewTicker(discoveryPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.connectDiscoveryRound(ctx)
		}
	}
}

func (s *Server) connectDiscoveryRound(ctx context.Context) {
	if s.config.NodeConfig.GossipSub.Params.DHigh == nil {
		return
	}
	target := *s.config.NodeConfig.GossipSub.Params.DHigh
	if target <= 0 {
		return
	}
	topics := uniqueDiscoveryTopics(s.config.NodeConfig.GossipSub.Topics)
	if len(topics) == 0 {
		return
	}

	results := make([][]bootstrapNode, len(topics))
	var fetches sync.WaitGroup
	for index, topic := range topics {
		fetches.Add(1)
		go func() {
			defer fetches.Done()
			requestCtx, cancel := context.WithTimeout(ctx, discoveryRequestLimit)
			defer cancel()
			candidates, err := s.fetchDiscoveryPeers(requestCtx, topic)
			if err != nil {
				if ctx.Err() == nil && s.logger != nil {
					s.logger.Debug("discover topic peers", "topic", topic, "error", err)
				}
				return
			}
			results[index] = candidates
		}()
	}
	fetches.Wait()
	if ctx.Err() != nil {
		return
	}

	plans := make([]discoveryTopicPlan, 0, len(topics))
	required := 0
	for index, topic := range topics {
		needed := target
		if s.pubsub != nil {
			needed -= len(s.pubsub.ListPeers(topic))
		}
		if needed <= 0 {
			continue
		}
		plans = append(plans, discoveryTopicPlan{
			topic:      topic,
			needed:     needed,
			candidates: selectDiscoveryPeers(s.config.Node.ID, topic, results[index], len(results[index])),
		})
		required += needed
	}
	if required == 0 {
		return
	}
	budget := discoveryConnectionBudget(s.config.NodeConfig.Libp2p.ConnectionLimit, len(s.host.Network().Conns()), required)
	if budget == 0 {
		return
	}
	now := time.Now()
	s.pruneDiscoveryFailures(now)
	dialCtx, cancel := context.WithTimeout(ctx, discoveryRequestLimit)
	defer cancel()
	connectDiscoveryCandidates(dialCtx, plans, budget, func(peerID corepeer.ID) bool {
		if s.host.Network().Connectedness(peerID) == corenetwork.Connected {
			s.clearDiscoveryFailure(peerID)
			return true
		}
		return s.discoveryRetryPending(peerID, now)
	}, func(connectCtx context.Context, info corepeer.AddrInfo) error {
		err := s.host.Connect(connectCtx, info)
		if err == nil {
			s.clearDiscoveryFailure(info.ID)
			return nil
		}
		if ctx.Err() == nil {
			s.recordDiscoveryFailure(info.ID, time.Now().Add(discoveryRetryDelay))
			if s.logger != nil {
				s.logger.Debug("connect discovered topic peer", "peer", info.ID, "error", err)
			}
		}
		return err
	})

	// The scheduler waits only for results inside dialCtx. Host.Connect receives
	// that same context, so any in-flight dial is canceled at the round boundary.
	if dialCtx.Err() != nil && ctx.Err() == nil && s.logger != nil {
		s.logger.Debug("topic discovery dial round ended", "error", dialCtx.Err())
	}
}

func discoveryConnectionBudget(limit *int, current, required int) int {
	if required <= 0 {
		return 0
	}
	// hostOptions treats nil and zero as unlimited; discovery must preserve the
	// same meaning rather than turning an explicit zero into a zero-slot cap.
	if limit == nil || *limit == 0 {
		return required
	}
	return min(required, max(0, *limit-current))
}

// connectDiscoveryCandidates consumes topic queues in round-robin order. Only
// successful new transports consume a topic's deficit or the hard-cap budget;
// a failed dial therefore advances to the next rendezvous-ranked candidate.
func connectDiscoveryCandidates(ctx context.Context, plans []discoveryTopicPlan, budget int, connected func(corepeer.ID) bool, connect func(context.Context, corepeer.AddrInfo) error) {
	if budget <= 0 || len(plans) == 0 {
		return
	}
	planIndex := make(map[string]int, len(plans))
	peerTopics := make(map[corepeer.ID]map[string]struct{})
	resultCapacity := 0
	for index := range plans {
		planIndex[plans[index].topic] = index
		resultCapacity += len(plans[index].candidates)
		for _, candidate := range plans[index].candidates {
			info, err := addrInfo(candidate)
			if err != nil {
				continue
			}
			if peerTopics[info.ID] == nil {
				peerTopics[info.ID] = make(map[string]struct{})
			}
			peerTopics[info.ID][plans[index].topic] = struct{}{}
		}
	}
	if resultCapacity == 0 {
		return
	}

	results := make(chan discoveryDialResult, resultCapacity)
	attempted := make(map[corepeer.ID]struct{}, resultCapacity)
	reserved := make(map[string]int, len(plans))
	cursor := 0
	nextCandidate := func() (corepeer.AddrInfo, []string, bool) {
		for checked := 0; checked < len(plans); checked++ {
			index := (cursor + checked) % len(plans)
			plan := &plans[index]
			if plan.needed <= reserved[plan.topic] {
				continue
			}
			for plan.next < len(plan.candidates) {
				candidate := plan.candidates[plan.next]
				plan.next++
				info, err := addrInfo(candidate)
				if err != nil {
					continue
				}
				if _, exists := attempted[info.ID]; exists {
					continue
				}
				attempted[info.ID] = struct{}{}
				if connected != nil && connected(info.ID) {
					continue
				}
				covered := make([]string, 0, len(peerTopics[info.ID]))
				for topic := range peerTopics[info.ID] {
					other := &plans[planIndex[topic]]
					if other.needed > reserved[topic] {
						covered = append(covered, topic)
					}
				}
				if len(covered) == 0 {
					continue
				}
				sort.Strings(covered)
				for _, topic := range covered {
					reserved[topic]++
				}
				cursor = (index + 1) % len(plans)
				return info, covered, true
			}
		}
		return corepeer.AddrInfo{}, nil, false
	}

	successes := 0
	inFlight := 0
	for successes < budget {
		for inFlight < budget-successes {
			info, covered, ok := nextCandidate()
			if !ok {
				break
			}
			inFlight++
			go func() {
				results <- discoveryDialResult{peerID: info.ID, topics: covered, err: connect(ctx, info)}
			}()
		}
		if inFlight == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case result := <-results:
			inFlight--
			for _, topic := range result.topics {
				reserved[topic]--
			}
			if result.err != nil {
				continue
			}
			successes++
			for _, topic := range result.topics {
				plan := &plans[planIndex[topic]]
				if plan.needed > 0 {
					plan.needed--
				}
			}
		}
	}
}

func (s *Server) pruneDiscoveryFailures(now time.Time) {
	s.discoveryMu.Lock()
	defer s.discoveryMu.Unlock()
	for peerID, retryAt := range s.discoveryFailures {
		if !now.Before(retryAt) {
			delete(s.discoveryFailures, peerID)
		}
	}
}

func (s *Server) discoveryRetryPending(peerID corepeer.ID, now time.Time) bool {
	s.discoveryMu.Lock()
	defer s.discoveryMu.Unlock()
	retryAt, exists := s.discoveryFailures[peerID]
	return exists && now.Before(retryAt)
}

func (s *Server) recordDiscoveryFailure(peerID corepeer.ID, retryAt time.Time) {
	s.discoveryMu.Lock()
	defer s.discoveryMu.Unlock()
	if s.discoveryFailures == nil {
		s.discoveryFailures = make(map[corepeer.ID]time.Time)
	}
	s.discoveryFailures[peerID] = retryAt
}

func (s *Server) clearDiscoveryFailure(peerID corepeer.ID) {
	s.discoveryMu.Lock()
	defer s.discoveryMu.Unlock()
	delete(s.discoveryFailures, peerID)
}

func uniqueDiscoveryTopics(topics []string) []string {
	seen := make(map[string]struct{}, len(topics))
	result := make([]string, 0, len(topics))
	for _, topic := range topics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}
		if _, exists := seen[topic]; exists {
			continue
		}
		seen[topic] = struct{}{}
		result = append(result, topic)
	}
	sort.Strings(result)
	return result
}
