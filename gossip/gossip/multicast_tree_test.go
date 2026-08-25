/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"fmt"
	"testing"
	"time"

	pg "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/comm"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/filter"
	"github.com/hyperledger/fabric/gossip/gossip/channel"
	"github.com/hyperledger/fabric/gossip/protoext"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/stretchr/testify/require"
)

func TestMulticastTreeFanout(t *testing.T) {
	require.Equal(t, 0, multicastTreeFanout(0))
	require.Equal(t, 0, multicastTreeFanout(1))
	require.Equal(t, 1, multicastTreeFanout(2))
	require.Equal(t, 2, multicastTreeFanout(3))
	require.Equal(t, 3, multicastTreeFanout(9))
	require.Equal(t, 4, multicastTreeFanout(10))
	require.Equal(t, 10, multicastTreeFanout(100))
}

func TestMulticastTreeCoversAllPeersOnce(t *testing.T) {
	// 10 peers, source = p00. A sqrt(10)≈4-ary tree must reach everyone
	// in at most 2 hops and use exactly n-1 edges.
	members := makeMembers(10)
	root := members[0].PKIid

	reached := map[string]int{string(root): 0}
	frontier := []common.PKIidType{root}
	edges := 0
	depth := 0
	for len(frontier) > 0 && depth < 8 {
		depth++
		var next []common.PKIidType
		for _, self := range frontier {
			for _, child := range multicastTreeChildren(members, self, root, nil) {
				edges++
				id := string(child.PKIID)
				if _, dup := reached[id]; dup {
					t.Fatalf("peer %x reached twice", child.PKIID)
				}
				reached[id] = depth
				next = append(next, child.PKIID)
			}
		}
		frontier = next
	}

	require.Equal(t, 10, len(reached), "every peer must be reached")
	require.Equal(t, 9, edges, "tree must have n-1 edges")
	for id, hops := range reached {
		require.LessOrEqual(t, hops, 2, "peer %x took %d hops", id, hops)
	}
}

func TestMulticastTreeSourceIsAlwaysRoot(t *testing.T) {
	members := makeMembers(9)
	// Originating from a peer that is not the min PKI-ID must still
	// place that peer at the root (children in the first hop).
	source := members[5].PKIid
	children := multicastTreeChildren(members, source, source, nil)
	require.Len(t, children, multicastTreeFanout(9))

	leaf := children[len(children)-1].PKIID
	require.Empty(t, multicastTreeChildren(members, leaf, source, nil))
}

func TestMulticastTreeExcludesSender(t *testing.T) {
	members := makeMembers(4)
	root := members[0].PKIid
	children := multicastTreeChildren(members, root, root, members[1].PKIid)
	for _, child := range children {
		require.NotEqual(t, []byte(members[1].PKIid), []byte(child.PKIID))
	}
}

func TestResolveTreeRootPrefersNonce(t *testing.T) {
	members := makeMembers(4)
	nonce := pkiIDToNonce(members[2].PKIid)
	require.Equal(t, members[2].PKIid, resolveTreeRoot(nonce, members, members[0].PKIid))
	require.Equal(t, members[0].PKIid, resolveTreeRoot(0, members, members[0].PKIid))
	require.Equal(t, members[0].PKIid, resolveTreeRoot(0, members, nil))
}

func TestIsBlockCommitMsg(t *testing.T) {
	require.False(t, isBlockCommitMsg(nil))
	require.False(t, isBlockCommitMsg(&pg.GossipMessage{
		Channel: []byte("A"),
		Content: &pg.GossipMessage_LeadershipMsg{LeadershipMsg: &pg.LeadershipMessage{}},
	}))
	require.False(t, isBlockCommitMsg(&pg.GossipMessage{
		Content: &pg.GossipMessage_DataMsg{DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: 1}}},
	}))
	require.True(t, isBlockCommitMsg(&pg.GossipMessage{
		Channel: []byte("A"),
		Content: &pg.GossipMessage_DataMsg{DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: 1}}},
	}))
}

func TestGossipSendsBlockToTreeChildrenOnly(t *testing.T) {
	node := newMulticastTreeTestNode(t, membersOf("self", "a", "b", "c", "d", "e", "f", "g", "h")...)
	var sentTo []*comm.RemotePeer
	node.comm = &multicastTreeMockComm{
		pkiID: []byte("self"),
		sendFn: func(_ *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
			sentTo = append(sentTo, peers...)
		},
	}

	node.Gossip(&pg.GossipMessage{
		Channel: []byte("A"),
		Tag:     pg.GossipMessage_CHAN_AND_ORG,
		Content: &pg.GossipMessage_DataMsg{
			DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: 1}},
		},
	})

	// 9 members, fanout = 3. Source is root so first hop is 3 children, not all 8 peers.
	require.Len(t, sentTo, 3)
	require.Zero(t, node.emitter.Size(), "block must bypass the batching emitter")
}

func TestForwardSendsBlockToTreeChildrenOnly(t *testing.T) {
	// self="m1" sits immediately after source="m0" in PKI order, so it is
	// an interior node of the source-rooted tree and must fan out.
	node := newMulticastTreeTestNode(t, membersOf("m1", "m0", "m2", "m3", "m4", "m5", "m6", "m7", "m8")...)
	var sentTo []*comm.RemotePeer
	node.comm = &multicastTreeMockComm{
		pkiID: []byte("m1"),
		sendFn: func(_ *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
			sentTo = append(sentTo, peers...)
		},
	}
	adapter := &gossipAdapterImpl{Node: node}

	src := []byte("m0")
	adapter.Forward(&multicastTreeReceivedMsg{
		msg: &protoext.SignedGossipMessage{
			GossipMessage: &pg.GossipMessage{
				Nonce:   pkiIDToNonce(src),
				Channel: []byte("A"),
				Tag:     pg.GossipMessage_CHAN_AND_ORG,
				Content: &pg.GossipMessage_DataMsg{
					DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: 4}},
				},
			},
		},
		sender: src,
	})

	require.NotEmpty(t, sentTo)
	require.LessOrEqual(t, len(sentTo), multicastTreeFanout(9))
	for _, peer := range sentTo {
		require.NotEqual(t, src, []byte(peer.PKIID))
	}
	require.Zero(t, node.emitter.Size(), "block forwards must not use the batching emitter")
}

func TestForwardStillGossipsNonBlockMessages(t *testing.T) {
	node := newMulticastTreeTestNode(t, membersOf("self", "a")...)
	adapter := &gossipAdapterImpl{Node: node}

	adapter.Forward(&multicastTreeReceivedMsg{
		msg: &protoext.SignedGossipMessage{
			GossipMessage: &pg.GossipMessage{
				Channel: []byte("A"),
				Tag:     pg.GossipMessage_CHAN_AND_ORG,
				Content: &pg.GossipMessage_LeadershipMsg{LeadershipMsg: &pg.LeadershipMessage{}},
			},
		},
		sender: []byte("a"),
	})

	require.Equal(t, 1, node.emitter.Size())
}

func makeMembers(n int) []discovery.NetworkMember {
	members := make([]discovery.NetworkMember, n)
	for i := 0; i < n; i++ {
		members[i] = discovery.NetworkMember{PKIid: []byte(fmt.Sprintf("p%02d", i))}
	}
	return members
}

func membersOf(ids ...string) []discovery.NetworkMember {
	members := make([]discovery.NetworkMember, len(ids))
	for i, id := range ids {
		members[i] = discovery.NetworkMember{PKIid: []byte(id)}
	}
	return members
}

func newMulticastTreeTestNode(t *testing.T, members ...discovery.NetworkMember) *Node {
	t.Helper()
	self := members[0]
	others := members[1:]
	node := &Node{
		conf:       &Config{PropagateIterations: 1, PropagatePeerNum: 3},
		logger:     util.GetLogger(util.GossipLogger, "test"),
		selfOrg:    api.OrgIdentityType("org"),
		idMapper:   &multicastTreeStubMapper{},
		secAdvisor: &multicastTreeStubAdvisor{},
		chanState: &channelState{channels: map[string]channel.GossipChannel{
			"A": &multicastTreeMockChannel{},
		}},
		disc: &multicastTreeMockDiscovery{self: self, members: others},
	}
	node.chanState.g = node
	node.emitter = newBatchingEmitter(1, 10, time.Hour, func([]interface{}) {})
	t.Cleanup(node.emitter.Stop)
	return node
}

type multicastTreeStubMapper struct{}

func (m *multicastTreeStubMapper) Put(common.PKIidType, api.PeerIdentityType) error { return nil }
func (m *multicastTreeStubMapper) Get(pkiID common.PKIidType) (api.PeerIdentityType, error) {
	return api.PeerIdentityType(pkiID), nil
}
func (m *multicastTreeStubMapper) Sign(msg []byte) ([]byte, error) { return msg, nil }
func (m *multicastTreeStubMapper) Verify(_, _, _ []byte) error     { return nil }
func (m *multicastTreeStubMapper) GetPKIidOfCert(id api.PeerIdentityType) common.PKIidType {
	return common.PKIidType(id)
}
func (m *multicastTreeStubMapper) SuspectPeers(api.PeerSuspector) {}
func (m *multicastTreeStubMapper) IdentityInfo() api.PeerIdentitySet {
	return nil
}
func (m *multicastTreeStubMapper) Stop() {}

type multicastTreeStubAdvisor struct{}

func (a *multicastTreeStubAdvisor) OrgByPeerIdentity(api.PeerIdentityType) api.OrgIdentityType {
	return api.OrgIdentityType("org")
}

type multicastTreeMockComm struct {
	pkiID  common.PKIidType
	sendFn func(msg *protoext.SignedGossipMessage, peers ...*comm.RemotePeer)
}

func (m *multicastTreeMockComm) GetPKIid() common.PKIidType { return m.pkiID }
func (m *multicastTreeMockComm) Send(msg *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
	if m.sendFn != nil {
		m.sendFn(msg, peers...)
	}
}
func (m *multicastTreeMockComm) SendWithAck(msg *protoext.SignedGossipMessage, _ time.Duration, _ int, peers ...*comm.RemotePeer) comm.AggregatedSendResult {
	return nil
}
func (m *multicastTreeMockComm) Probe(peer *comm.RemotePeer) error { return nil }
func (m *multicastTreeMockComm) Handshake(peer *comm.RemotePeer) (api.PeerIdentityType, error) {
	return nil, nil
}
func (m *multicastTreeMockComm) Accept(common.MessageAcceptor) <-chan protoext.ReceivedMessage {
	return nil
}
func (m *multicastTreeMockComm) PresumedDead() <-chan common.PKIidType { return nil }
func (m *multicastTreeMockComm) IdentitySwitch() chan common.PKIidType { return nil }
func (m *multicastTreeMockComm) CloseConn(peer *comm.RemotePeer)       {}
func (m *multicastTreeMockComm) Stop()                                 {}

type multicastTreeMockDiscovery struct {
	discovery.Discovery
	self    discovery.NetworkMember
	members []discovery.NetworkMember
}

func (m *multicastTreeMockDiscovery) GetMembership() []discovery.NetworkMember { return m.members }
func (m *multicastTreeMockDiscovery) Self() discovery.NetworkMember            { return m.self }

type multicastTreeMockChannel struct{}

func (m *multicastTreeMockChannel) Self() *protoext.SignedGossipMessage { return nil }
func (m *multicastTreeMockChannel) GetPeers() []discovery.NetworkMember { return nil }
func (m *multicastTreeMockChannel) PeerFilter(api.SubChannelSelectionCriteria) filter.RoutingFilter {
	return nil
}
func (m *multicastTreeMockChannel) IsMemberInChan(discovery.NetworkMember) bool     { return true }
func (m *multicastTreeMockChannel) UpdateLedgerHeight(uint64)                       {}
func (m *multicastTreeMockChannel) UpdateChaincodes([]*pg.Chaincode)                {}
func (m *multicastTreeMockChannel) IsOrgInChannel(api.OrgIdentityType) bool         { return true }
func (m *multicastTreeMockChannel) EligibleForChannel(discovery.NetworkMember) bool { return true }
func (m *multicastTreeMockChannel) HandleMessage(protoext.ReceivedMessage)          {}
func (m *multicastTreeMockChannel) AddToMsgStore(*protoext.SignedGossipMessage)     {}
func (m *multicastTreeMockChannel) ConfigureChannel(api.JoinChannelMessage)         {}
func (m *multicastTreeMockChannel) LeaveChannel()                                   {}
func (m *multicastTreeMockChannel) Stop()                                           {}

type multicastTreeReceivedMsg struct {
	msg    *protoext.SignedGossipMessage
	sender common.PKIidType
}

func (m *multicastTreeReceivedMsg) GetSourceEnvelope() *pg.Envelope { return nil }
func (m *multicastTreeReceivedMsg) GetGossipMessage() *protoext.SignedGossipMessage {
	return m.msg
}
func (m *multicastTreeReceivedMsg) GetConnectionInfo() *protoext.ConnectionInfo {
	return &protoext.ConnectionInfo{ID: m.sender}
}
func (m *multicastTreeReceivedMsg) Respond(msg *pg.GossipMessage) {}
func (m *multicastTreeReceivedMsg) Ack(error)                     {}
