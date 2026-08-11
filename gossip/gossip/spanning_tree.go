/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"time"

	pg "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/comm"
)

// SpanningTreeMsg action encoding reuses PathCost/Distance sentinels so we do not
// need a regenerated protobuf descriptor. Beacons never use PathCost == stActionSentinel.
const stActionSentinel = math.MaxUint32

const (
	stDistanceAttach = uint32(0) // child -> parent: request attachment
	stDistanceDetach = uint32(1) // child -> parent: detach
	stDistanceAccept = uint32(2) // parent -> child: accept attachment
	stDistanceReject = uint32(3) // parent -> child: reject (degree full)
)

// Default link cost when RTT has not been measured yet (milliseconds-equivalent).
const defaultLinkCost uint32 = 100

// spanningTreeState holds the local view of the dissemination tree:
// one parent (or root), an explicit children set, and metrics used for parent choice.
type spanningTreeState struct {
	enabled     bool
	maxChildren int
	selfPKIID   []byte

	parent        []byte
	pendingParent []byte // waiting for ChildAttachAccept
	bestPathCost  uint32
	bestDistance  uint32
	rootPKIID     []byte
	rootIncNum    uint64
	rootSeqNum    uint64
	isRoot        bool
	rootIncarnation uint64

	children map[string]struct{}
	seen     map[string]struct{}
	linkCost map[string]uint32 // peer PKI-ID -> measured/estimated link cost

	mu sync.RWMutex
}

func newSpanningTreeState(selfPKIID []byte, enabled bool, maxChildren int) *spanningTreeState {
	if maxChildren <= 0 {
		maxChildren = 8
	}
	return &spanningTreeState{
		enabled:     enabled,
		maxChildren: maxChildren,
		selfPKIID:   append([]byte(nil), selfPKIID...),
		children:    make(map[string]struct{}),
		seen:        make(map[string]struct{}),
		linkCost:    make(map[string]uint32),
	}
}

func isSpanningTreeAction(st *pg.SpanningTreeMsg) bool {
	return st != nil && st.GetPathCost() == stActionSentinel
}

func spanningTreeAction(st *pg.SpanningTreeMsg) uint32 {
	if !isSpanningTreeAction(st) {
		return math.MaxUint32
	}
	return st.GetDistance()
}

func newSpanningTreeActionMsg(action uint32, rootPKIID []byte, rootInc, rootSeq uint64, sender []byte) *pg.GossipMessage {
	return &pg.GossipMessage{
		Nonce: 0,
		Tag:   pg.GossipMessage_EMPTY,
		Content: &pg.GossipMessage_SpanningTree{
			SpanningTree: &pg.SpanningTreeMsg{
				RootPkiId:   append([]byte(nil), rootPKIID...),
				RootIncNum:  rootInc,
				RootSeqNum:  rootSeq,
				Distance:    action,
				SenderPkiId: append([]byte(nil), sender...),
				PathCost:    stActionSentinel,
			},
		},
	}
}

func newSpanningTreeBeaconMsg(rootPKIID []byte, rootInc, rootSeq uint64, distance, pathCost uint32, sender []byte) *pg.GossipMessage {
	if pathCost == stActionSentinel {
		pathCost = stActionSentinel - 1
	}
	return &pg.GossipMessage{
		Nonce: 0,
		Tag:   pg.GossipMessage_EMPTY,
		Content: &pg.GossipMessage_SpanningTree{
			SpanningTree: &pg.SpanningTreeMsg{
				RootPkiId:   append([]byte(nil), rootPKIID...),
				RootIncNum:  rootInc,
				RootSeqNum:  rootSeq,
				Distance:    distance,
				SenderPkiId: append([]byte(nil), sender...),
				PathCost:    pathCost,
			},
		},
	}
}

func (s *spanningTreeState) Enabled() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled
}

// Ready reports whether block dissemination may use the tree:
// the peer is the root, or it has an accepted parent.
func (s *spanningTreeState) Ready() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.enabled {
		return false
	}
	return s.isRoot || len(s.parent) > 0
}

func (s *spanningTreeState) IsRoot() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled && s.isRoot
}

func (s *spanningTreeState) Parent() []byte {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]byte(nil), s.parent...)
}

func (s *spanningTreeState) RootMetadata() (root []byte, inc, seq uint64, distance, cost uint32) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]byte(nil), s.rootPKIID...), s.rootIncNum, s.rootSeqNum, s.bestDistance, s.bestPathCost
}

func (s *spanningTreeState) SetLinkCost(peer []byte, cost uint32) {
	if s == nil || len(peer) == 0 {
		return
	}
	if cost == 0 {
		cost = 1
	}
	if cost >= stActionSentinel {
		cost = stActionSentinel - 1
	}
	s.mu.Lock()
	s.linkCost[string(peer)] = cost
	s.mu.Unlock()
}

func (s *spanningTreeState) linkCostOf(peer []byte) uint32 {
	if c, ok := s.linkCost[string(peer)]; ok {
		return c
	}
	return defaultLinkCost
}

// BecomeRoot installs this peer as the spanning-tree root.
func (s *spanningTreeState) BecomeRoot(incarnation uint64) (seq uint64, changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return 0, false
	}
	changed = !s.isRoot || !bytes.Equal(s.rootPKIID, s.selfPKIID) || s.rootIncNum != incarnation
	s.isRoot = true
	s.rootPKIID = append([]byte(nil), s.selfPKIID...)
	s.rootIncNum = incarnation
	s.rootIncarnation = incarnation
	if changed {
		s.rootSeqNum++
		s.parent = nil
		s.pendingParent = nil
		s.bestDistance = 0
		s.bestPathCost = 0
	}
	return s.rootSeqNum, changed
}

// RelinquishRoot clears root role when another peer should be root.
func (s *spanningTreeState) RelinquishRoot() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isRoot = false
}

// NextBeaconSeq bumps the root sequence for a periodic beacon (root only).
func (s *spanningTreeState) NextBeaconSeq() (root []byte, inc, seq uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled || !s.isRoot {
		return nil, 0, 0, false
	}
	s.rootSeqNum++
	return append([]byte(nil), s.rootPKIID...), s.rootIncNum, s.rootSeqNum, true
}

type parentSwitch struct {
	OldParent []byte
	NewParent []byte
	Root      []byte
	RootInc   uint64
	RootSeq   uint64
	Changed   bool
}

// handleBeacon updates parent candidacy from a relayed/root beacon.
// It does not mutate children; children are only changed via explicit join/leave.
// Returns a parentSwitch when the peer should (re)attach to a better parent.
func (s *spanningTreeState) handleBeacon(msg *pg.GossipMessage, sender []byte) parentSwitch {
	st := msg.GetSpanningTree()
	var out parentSwitch
	if s == nil || st == nil || isSpanningTreeAction(st) || len(sender) == 0 {
		return out
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return out
	}

	key := fmt.Sprintf("%x:%d:%d:%x", st.GetRootPkiId(), st.GetRootIncNum(), st.GetRootSeqNum(), sender)
	if _, exists := s.seen[key]; exists {
		return out
	}
	s.seen[key] = struct{}{}
	// Bound seen-map growth: drop arbitrary entries when large.
	if len(s.seen) > 4096 {
		s.seen = map[string]struct{}{key: {}}
	}

	// Prefer newer root incarnation; ignore stale roots.
	if len(s.rootPKIID) > 0 {
		rootCmp := bytes.Compare(st.GetRootPkiId(), s.rootPKIID)
		if rootCmp < 0 && st.GetRootIncNum() < s.rootIncNum {
			// Older announcement from a lesser root — ignore unless we have no parent and are not root.
			if s.isRoot || len(s.parent) > 0 || len(s.pendingParent) > 0 {
				return out
			}
		}
		if bytes.Equal(st.GetRootPkiId(), s.rootPKIID) && st.GetRootIncNum() < s.rootIncNum {
			return out
		}
	}

	// If we are root and hear a better (lower PKI) root, relinquish.
	if s.isRoot && !bytes.Equal(st.GetRootPkiId(), s.selfPKIID) {
		if bytes.Compare(st.GetRootPkiId(), s.selfPKIID) < 0 {
			s.isRoot = false
		} else {
			return out
		}
	}

	candidateCost := st.GetPathCost() + s.linkCostOf(sender)
	if candidateCost < st.GetPathCost() { // overflow guard
		candidateCost = stActionSentinel - 1
	}
	candidateDistance := st.GetDistance() + 1

	better := false
	switch {
	case len(s.parent) == 0 && len(s.pendingParent) == 0 && !s.isRoot:
		better = true
	case bytes.Equal(s.parent, sender) || bytes.Equal(s.pendingParent, sender):
		// Refresh metrics from current parent.
		s.bestPathCost = candidateCost
		s.bestDistance = candidateDistance
		s.rootPKIID = append([]byte(nil), st.GetRootPkiId()...)
		s.rootIncNum = st.GetRootIncNum()
		s.rootSeqNum = st.GetRootSeqNum()
		return out
	case candidateCost < s.bestPathCost:
		better = true
	case candidateCost == s.bestPathCost && candidateDistance < s.bestDistance:
		better = true
	}

	if !better {
		return out
	}

	out.OldParent = append([]byte(nil), s.parent...)
	if len(out.OldParent) == 0 {
		out.OldParent = append([]byte(nil), s.pendingParent...)
	}
	out.NewParent = append([]byte(nil), sender...)
	out.Root = append([]byte(nil), st.GetRootPkiId()...)
	out.RootInc = st.GetRootIncNum()
	out.RootSeq = st.GetRootSeqNum()
	out.Changed = true

	s.pendingParent = append([]byte(nil), sender...)
	s.parent = nil
	s.rootPKIID = out.Root
	s.rootIncNum = out.RootInc
	s.rootSeqNum = out.RootSeq
	s.bestPathCost = candidateCost
	s.bestDistance = candidateDistance
	s.isRoot = false
	return out
}

// handleAttachRequest processes ChildAttach from a peer. Returns accept/reject and whether accepted.
func (s *spanningTreeState) handleAttachRequest(sender []byte) (accept bool) {
	if s == nil || len(sender) == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return false
	}
	// Never accept our parent/pending parent as a child (cycle prevention).
	if bytes.Equal(s.parent, sender) || bytes.Equal(s.pendingParent, sender) {
		return false
	}
	if _, exists := s.children[string(sender)]; exists {
		return true
	}
	if len(s.children) >= s.maxChildren {
		return false
	}
	s.children[string(sender)] = struct{}{}
	return true
}

func (s *spanningTreeState) handleDetach(sender []byte) {
	if s == nil || len(sender) == 0 {
		return
	}
	s.mu.Lock()
	delete(s.children, string(sender))
	s.mu.Unlock()
}

// handleAttachAccept confirms pending parent.
func (s *spanningTreeState) handleAttachAccept(sender []byte) bool {
	if s == nil || len(sender) == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !bytes.Equal(s.pendingParent, sender) {
		return false
	}
	s.parent = append([]byte(nil), sender...)
	s.pendingParent = nil
	delete(s.children, string(sender))
	return true
}

// handleAttachReject clears pending parent so another candidate can be tried.
func (s *spanningTreeState) handleAttachReject(sender []byte) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if bytes.Equal(s.pendingParent, sender) {
		s.pendingParent = nil
		// Invalidate cost so the next beacon can pick a different parent.
		s.bestPathCost = stActionSentinel - 1
		s.bestDistance = math.MaxUint32
	}
}

// OnPeerDead removes a dead peer from parent/children state.
func (s *spanningTreeState) OnPeerDead(peer []byte) (lostParent bool) {
	if s == nil || len(peer) == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.children, string(peer))
	delete(s.linkCost, string(peer))
	if bytes.Equal(s.parent, peer) || bytes.Equal(s.pendingParent, peer) {
		s.parent = nil
		s.pendingParent = nil
		s.bestPathCost = stActionSentinel - 1
		s.bestDistance = math.MaxUint32
		return true
	}
	return false
}

func (s *spanningTreeState) selectPeers(peers []*comm.RemotePeer) []*comm.RemotePeer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	var selected []*comm.RemotePeer
	for _, peer := range peers {
		if _, isChild := s.children[string(peer.PKIID)]; isChild {
			selected = append(selected, peer)
		}
	}
	return selected
}

func (s *spanningTreeState) ChildCount() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.children)
}

// pickRootPKIID chooses the lexicographically smallest PKI-ID among self and members.
func pickRootPKIID(self []byte, members [][]byte) []byte {
	best := append([]byte(nil), self...)
	for _, m := range members {
		if len(m) == 0 {
			continue
		}
		if len(best) == 0 || bytes.Compare(m, best) < 0 {
			best = append([]byte(nil), m...)
		}
	}
	return best
}

// rttToLinkCost maps a measured RTT to a path-cost contribution.
func rttToLinkCost(d time.Duration) uint32 {
	if d <= 0 {
		return 1
	}
	ms := d.Milliseconds()
	if ms <= 0 {
		return 1
	}
	if ms > int64(stActionSentinel-2) {
		return stActionSentinel - 2
	}
	return uint32(ms)
}
