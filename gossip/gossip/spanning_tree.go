/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	pg "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/comm"
	"github.com/hyperledger/fabric/gossip/common"
)

const (
	defMaxSpanningTreeFanout           = 8
	defMaxSpanningTreeDistance         = 16
	defSpanningTreePropagateInterval = 5 * time.Second
	defSpanningTreeEdgeCost          = uint32(1)
	defSpanningTreeSeenRetention     = 1024
)

// spanningTreeState tracks per-channel spanning trees used to route block DataMsgs.
type spanningTreeState struct {
	enabled     bool
	maxFanout   int
	maxDistance uint32
	selfPKIID   []byte
	rootIncNum  uint64

	mu       sync.RWMutex
	channels map[string]*channelSpanningTree
	// edgeCosts maps peer PKI-ID to the measured (or default) cost of the link to that peer.
	edgeCosts map[string]uint32
}

type channelSpanningTree struct {
	parent       []byte
	bestPathCost uint32
	bestDistance uint32
	rootPKIID    []byte
	rootIncNum   uint64
	rootSeqNum   uint64
	children     map[string]struct{}
	seen         map[string]struct{}
	isRoot       bool
	seqNum       uint64
}

func newSpanningTreeState(selfPKIID []byte, enabled bool, maxFanout int, maxDistance uint32) *spanningTreeState {
	if maxFanout <= 0 {
		maxFanout = defMaxSpanningTreeFanout
	}
	if maxDistance == 0 {
		maxDistance = defMaxSpanningTreeDistance
	}
	return &spanningTreeState{
		enabled:     enabled,
		maxFanout:   maxFanout,
		maxDistance: maxDistance,
		selfPKIID:   append([]byte(nil), selfPKIID...),
		rootIncNum:  uint64(time.Now().UnixNano()),
		channels:    make(map[string]*channelSpanningTree),
		edgeCosts:   make(map[string]uint32),
	}
}

func (s *spanningTreeState) Enabled() bool {
	return s != nil && s.enabled
}

func (s *spanningTreeState) getOrCreateLocked(channel string) *channelSpanningTree {
	tree, ok := s.channels[channel]
	if ok {
		return tree
	}
	tree = &channelSpanningTree{
		children: make(map[string]struct{}),
		seen:     make(map[string]struct{}),
	}
	s.channels[channel] = tree
	return tree
}

// SetRoot marks this peer as the spanning-tree root for the channel (or clears that role).
func (s *spanningTreeState) SetRoot(channel string, isRoot bool) {
	if s == nil || !s.enabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tree := s.getOrCreateLocked(channel)
	tree.isRoot = isRoot
	if isRoot {
		tree.parent = nil
		tree.bestPathCost = 0
		tree.bestDistance = 0
		tree.rootPKIID = append([]byte(nil), s.selfPKIID...)
		tree.rootIncNum = s.rootIncNum
		tree.seqNum++
		tree.rootSeqNum = tree.seqNum
		delete(tree.children, string(s.selfPKIID))
	}
}

// IsRoot reports whether this peer is currently the spanning-tree root for the channel.
func (s *spanningTreeState) IsRoot(channel string) bool {
	if s == nil || !s.enabled {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	tree, ok := s.channels[channel]
	return ok && tree.isRoot
}

// RecordEdgeCost stores a measured path cost for the link to peer.
func (s *spanningTreeState) RecordEdgeCost(peer common.PKIidType, cost uint32) {
	if s == nil || !s.enabled || len(peer) == 0 {
		return
	}
	if cost == 0 {
		cost = defSpanningTreeEdgeCost
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.edgeCosts[string(peer)] = cost
}

func (s *spanningTreeState) edgeCostLocked(peer []byte) uint32 {
	if cost, ok := s.edgeCosts[string(peer)]; ok && cost > 0 {
		return cost
	}
	return defSpanningTreeEdgeCost
}

// RootChannels returns channel IDs for which this peer is currently the spanning-tree root.
func (s *spanningTreeState) RootChannels() []string {
	if s == nil || !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var channels []string
	for channel, tree := range s.channels {
		if tree.isRoot {
			channels = append(channels, channel)
		}
	}
	return channels
}

// channelsWithParent returns non-root channels that currently have an adopted parent.
func (s *spanningTreeState) channelsWithParent() []string {
	if s == nil || !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var channels []string
	for channel, tree := range s.channels {
		if !tree.isRoot && len(tree.parent) > 0 {
			channels = append(channels, channel)
		}
	}
	return channels
}

// BuildRootAdvertisement creates a spanning-tree control message for a channel this peer roots.
// Returns nil when this peer is not the root for the channel.
func (s *spanningTreeState) BuildRootAdvertisement(channel string) *pg.GossipMessage {
	if s == nil || !s.enabled {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tree, ok := s.channels[channel]
	if !ok || !tree.isRoot {
		return nil
	}
	tree.seqNum++
	tree.rootSeqNum = tree.seqNum
	return s.advertisementLocked(channel, tree, 0, 0)
}

// BuildParentAdvertisement re-advertises the best known path for a non-root channel.
func (s *spanningTreeState) BuildParentAdvertisement(channel string) *pg.GossipMessage {
	if s == nil || !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	tree, ok := s.channels[channel]
	if !ok || tree.isRoot || len(tree.parent) == 0 || len(tree.rootPKIID) == 0 {
		return nil
	}
	return s.advertisementLocked(channel, tree, tree.bestDistance, tree.bestPathCost)
}

func (s *spanningTreeState) advertisementLocked(channel string, tree *channelSpanningTree, distance uint32, pathCost uint32) *pg.GossipMessage {
	return &pg.GossipMessage{
		Nonce:   0,
		Tag:     pg.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(channel),
		Content: &pg.GossipMessage_SpanningTree{
			SpanningTree: &pg.SpanningTreeMsg{
				RootPkiId:   append([]byte(nil), tree.rootPKIID...),
				RootIncNum:  tree.rootIncNum,
				RootSeqNum:  tree.rootSeqNum,
				Distance:    distance,
				SenderPkiId: append([]byte(nil), s.selfPKIID...),
				PathCost:    pathCost,
			},
		},
	}
}

// handle processes a spanning-tree advertisement for a channel.
// When the local parent changes, it returns a re-advertisement with Distance/PathCost updated.
func (s *spanningTreeState) handle(channel string, msg *pg.GossipMessage, sender []byte) (bool, *pg.GossipMessage) {
	if s == nil || !s.enabled {
		return false, nil
	}
	st := msg.GetSpanningTree()
	if st == nil || len(channel) == 0 {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tree := s.getOrCreateLocked(channel)
	if tree.isRoot {
		// Roots do not adopt parents; treat advertisers as children.
		if len(sender) > 0 && !bytes.Equal(sender, s.selfPKIID) {
			tree.children[string(sender)] = struct{}{}
		}
		return false, nil
	}

	key := fmt.Sprintf("%s:%d:%d:%s", string(st.GetRootPkiId()), st.GetRootIncNum(), st.GetRootSeqNum(), string(sender))
	if _, seen := tree.seen[key]; seen {
		return false, nil
	}
	if len(tree.seen) >= defSpanningTreeSeenRetention {
		tree.seen = make(map[string]struct{})
	}
	tree.seen[key] = struct{}{}

	candidateDistance := st.GetDistance()
	if candidateDistance >= s.maxDistance {
		if len(sender) > 0 {
			tree.children[string(sender)] = struct{}{}
		}
		return false, nil
	}

	edgeCost := s.edgeCostLocked(sender)
	candidateCost := st.GetPathCost() + edgeCost
	candidateDistance++

	adopt := tree.parent == nil ||
		bytes.Equal(tree.parent, sender) ||
		candidateCost < tree.bestPathCost ||
		(candidateCost == tree.bestPathCost && candidateDistance < tree.bestDistance)

	if !adopt {
		if len(sender) > 0 {
			tree.children[string(sender)] = struct{}{}
		}
		return false, nil
	}

	if len(tree.parent) > 0 && !bytes.Equal(tree.parent, sender) {
		tree.children[string(tree.parent)] = struct{}{}
	}
	tree.parent = append([]byte(nil), sender...)
	tree.rootPKIID = append([]byte(nil), st.GetRootPkiId()...)
	tree.rootIncNum = st.GetRootIncNum()
	tree.rootSeqNum = st.GetRootSeqNum()
	tree.bestPathCost = candidateCost
	tree.bestDistance = candidateDistance
	delete(tree.children, string(sender))

	return true, s.advertisementLocked(channel, tree, tree.bestDistance, tree.bestPathCost)
}

// selectPeers returns child peers capped by maxFanout, preferring lower edge cost.
func (s *spanningTreeState) selectPeers(channel string, peers []*comm.RemotePeer) []*comm.RemotePeer {
	if s == nil || !s.enabled {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	tree, ok := s.channels[channel]
	if !ok || len(tree.children) == 0 {
		return nil
	}

	type scoredPeer struct {
		peer *comm.RemotePeer
		cost uint32
	}
	var selected []scoredPeer
	for _, peer := range peers {
		if _, isChild := tree.children[string(peer.PKIID)]; !isChild {
			continue
		}
		selected = append(selected, scoredPeer{peer: peer, cost: s.edgeCostLocked(peer.PKIID)})
	}
	if len(selected) == 0 {
		return nil
	}

	// Prefer lower-cost children so the first maxFanout picks are latency-friendly.
	for i := 1; i < len(selected); i++ {
		for j := i; j > 0 && selected[j].cost < selected[j-1].cost; j-- {
			selected[j], selected[j-1] = selected[j-1], selected[j]
		}
	}
	limit := s.maxFanout
	if limit > len(selected) {
		limit = len(selected)
	}
	result := make([]*comm.RemotePeer, 0, limit)
	for i := 0; i < limit; i++ {
		result = append(result, selected[i].peer)
	}
	return result
}

// Prune removes peers that are no longer in membership from parent/children sets.
func (s *spanningTreeState) Prune(channel string, alive map[string]struct{}) {
	if s == nil || !s.enabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tree, ok := s.channels[channel]
	if !ok {
		return
	}
	if len(tree.parent) > 0 {
		if _, ok := alive[string(tree.parent)]; !ok {
			tree.parent = nil
			tree.bestPathCost = 0
			tree.bestDistance = 0
		}
	}
	for child := range tree.children {
		if _, ok := alive[child]; !ok {
			delete(tree.children, child)
		}
	}
}

// parentOf returns the parent PKI-ID for tests.
func (s *spanningTreeState) parentOf(channel string) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tree, ok := s.channels[channel]
	if !ok {
		return nil
	}
	return append([]byte(nil), tree.parent...)
}

// bestPathCostOf returns the best path cost for tests.
func (s *spanningTreeState) bestPathCostOf(channel string) uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tree, ok := s.channels[channel]
	if !ok {
		return 0
	}
	return tree.bestPathCost
}

// bestDistanceOf returns the best distance for tests.
func (s *spanningTreeState) bestDistanceOf(channel string) uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tree, ok := s.channels[channel]
	if !ok {
		return 0
	}
	return tree.bestDistance
}

// addChildForTest injects a child for unit tests.
func (s *spanningTreeState) addChildForTest(channel string, child []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tree := s.getOrCreateLocked(channel)
	tree.children[string(child)] = struct{}{}
}
