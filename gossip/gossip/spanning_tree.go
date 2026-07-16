/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"bytes"
	"fmt"
	"sync"

	pg "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/comm"
)

type spanningTreeState struct {
	parent       []byte
	bestPathCost uint32
	bestDistance uint32
	rootPKIID    []byte
	rootIncNum   uint64
	rootSeqNum   uint64
	children     map[string]struct{}
	seen         map[string]struct{}
	mu           sync.RWMutex
}

func newSpanningTreeState() *spanningTreeState {
	return &spanningTreeState{children: make(map[string]struct{}), seen: make(map[string]struct{})}
}

func (s *spanningTreeState) handle(msg *pg.GossipMessage, sender []byte) bool {
	st := msg.GetSpanningTree()
	if st == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s:%d:%d:%s", string(st.GetRootPkiId()), st.GetRootIncNum(), st.GetRootSeqNum(), string(sender))
	if _, seen := s.seen[key]; seen {
		return false
	}
	s.seen[key] = struct{}{}

	candidateCost := st.GetPathCost()
	candidateDistance := st.GetDistance()
	if s.parent == nil || bytes.Equal(s.parent, sender) || candidateCost < s.bestPathCost || (candidateCost == s.bestPathCost && candidateDistance < s.bestDistance) {
		if len(s.parent) > 0 && !bytes.Equal(s.parent, sender) {
			s.children[string(s.parent)] = struct{}{}
		}
		s.parent = append([]byte(nil), sender...)
		s.rootPKIID = append([]byte(nil), st.GetRootPkiId()...)
		s.rootIncNum = st.GetRootIncNum()
		s.rootSeqNum = st.GetRootSeqNum()
		s.bestPathCost = candidateCost
		s.bestDistance = candidateDistance
		delete(s.children, string(sender))
		return true
	}

	if len(sender) > 0 {
		s.children[string(sender)] = struct{}{}
	}
	return false
}

func (s *spanningTreeState) selectPeers(peers []*comm.RemotePeer) []*comm.RemotePeer {
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
