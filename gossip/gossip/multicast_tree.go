/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"bytes"
	"hash/fnv"
	"math"
	"sort"

	pg "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/comm"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/filter"
	"github.com/hyperledger/fabric/gossip/protoext"
)

// isBlockCommitMsg reports whether msg is a channel block DataMsg that
// should be disseminated over the intra-org multicast tree.
func isBlockCommitMsg(msg *pg.GossipMessage) bool {
	if msg == nil || !protoext.IsDataMsg(msg) || len(msg.Channel) == 0 {
		return false
	}
	dataMsg := msg.GetDataMsg()
	return dataMsg != nil && dataMsg.Payload != nil
}

// multicastTreeFanout returns the k-ary degree that minimizes block
// propagation latency for a membership of n peers (including the source).
//
// Blocks are large, so dissemination is bandwidth-bound. A star (k = n-1)
// dumps the entire payload onto the source NIC. A binary tree needs
// ceil(log2 n) hops. Choosing k = ceil(sqrt(n)) yields a balanced tree
// of height at most 2 for any n, so each hop sends only ~sqrt(n) copies
// while the number of serial hops stays constant.
func multicastTreeFanout(n int) int {
	if n <= 1 {
		return 0
	}
	k := int(math.Ceil(math.Sqrt(float64(n))))
	if k < 1 {
		return 1
	}
	if k > n-1 {
		return n - 1
	}
	return k
}

// pkiIDToNonce encodes a PKI-ID into GossipMessage.Nonce so every hop can
// reconstruct the same source-rooted tree without a protobuf change.
func pkiIDToNonce(id common.PKIidType) uint64 {
	if len(id) == 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write(id)
	nonce := h.Sum64()
	if nonce == 0 {
		return 1
	}
	return nonce
}

func minPKIMember(members []discovery.NetworkMember) discovery.NetworkMember {
	best := members[0]
	for _, m := range members[1:] {
		if bytes.Compare(m.PKIid, best.PKIid) < 0 {
			best = m
		}
	}
	return best
}

func indexOfMember(members []discovery.NetworkMember, id common.PKIidType) int {
	for i, m := range members {
		if bytes.Equal(m.PKIid, id) {
			return i
		}
	}
	return -1
}

func sortMembersByPKIID(members []discovery.NetworkMember) []discovery.NetworkMember {
	sorted := append([]discovery.NetworkMember(nil), members...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].PKIid, sorted[j].PKIid) < 0
	})
	return sorted
}

// resolveTreeRoot picks the source of a source-rooted multicast tree.
// Nonce is a fingerprint of the originating peer's PKI-ID. If it cannot
// be resolved (Nonce 0 or a membership view that does not yet include
// the source), fall back to fallback, then to the lexicographically
// smallest PKI-ID so every peer still agrees on one tree.
func resolveTreeRoot(nonce uint64, members []discovery.NetworkMember, fallback common.PKIidType) common.PKIidType {
	if len(members) == 0 {
		return fallback
	}
	if nonce != 0 {
		if pkiIDToNonce(fallback) == nonce {
			return fallback
		}
		for _, m := range members {
			if pkiIDToNonce(m.PKIid) == nonce {
				return m.PKIid
			}
		}
	}
	if len(fallback) > 0 && indexOfMember(members, fallback) >= 0 {
		return fallback
	}
	return minPKIMember(members).PKIid
}

func rotateRootFirst(sorted []discovery.NetworkMember, root common.PKIidType) []discovery.NetworkMember {
	rootIdx := indexOfMember(sorted, root)
	if rootIdx <= 0 {
		return sorted
	}
	rotated := make([]discovery.NetworkMember, len(sorted))
	for i := range sorted {
		rotated[i] = sorted[(rootIdx+i)%len(sorted)]
	}
	return rotated
}

func collectTreeMembers(self discovery.NetworkMember, others []discovery.NetworkMember, rf filter.RoutingFilter) []discovery.NetworkMember {
	seen := map[string]struct{}{}
	if len(self.PKIid) > 0 {
		seen[string(self.PKIid)] = struct{}{}
	}
	out := make([]discovery.NetworkMember, 0, len(others)+1)
	if len(self.PKIid) > 0 {
		out = append(out, self)
	}
	for _, m := range others {
		if len(m.PKIid) == 0 {
			continue
		}
		if _, dup := seen[string(m.PKIid)]; dup {
			continue
		}
		if rf != nil && !rf(m) {
			continue
		}
		seen[string(m.PKIid)] = struct{}{}
		out = append(out, m)
	}
	return out
}

func remotePeerOf(m discovery.NetworkMember) *comm.RemotePeer {
	return &comm.RemotePeer{PKIID: m.PKIid, Endpoint: m.PreferredEndpoint()}
}

// multicastTreeChildren returns the next hops for self in a source-rooted
// k-ary multicast tree over members. exclude is omitted (the peer that
// just delivered the block).
func multicastTreeChildren(members []discovery.NetworkMember, self, root, exclude common.PKIidType) []*comm.RemotePeer {
	if len(members) <= 1 || len(self) == 0 {
		return nil
	}
	if len(root) == 0 {
		root = minPKIMember(members).PKIid
	}
	ordered := rotateRootFirst(sortMembersByPKIID(members), root)
	k := multicastTreeFanout(len(ordered))
	if k <= 0 {
		return nil
	}
	selfIdx := indexOfMember(ordered, self)
	if selfIdx < 0 {
		return nil
	}
	var children []*comm.RemotePeer
	for c := k*selfIdx + 1; c <= k*selfIdx+k && c < len(ordered); c++ {
		peer := ordered[c]
		if len(exclude) > 0 && bytes.Equal(peer.PKIid, exclude) {
			continue
		}
		children = append(children, remotePeerOf(peer))
	}
	return children
}
