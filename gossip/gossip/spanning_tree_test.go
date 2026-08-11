/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
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

func TestSpanningTreeAdoptsBestParentViaBeacon(t *testing.T) {
	self := []byte("self")
	state := newSpanningTreeState(self, true, 8)

	first := newSpanningTreeBeaconMsg([]byte("root"), 1, 1, 1, 10, []byte("peer-a"))
	ps := state.handleBeacon(first, []byte("peer-a"))
	require.True(t, ps.Changed)
	require.Equal(t, []byte("peer-a"), ps.NewParent)
	require.Nil(t, state.Parent()) // pending until accept

	require.True(t, state.handleAttachAccept([]byte("peer-a")))
	require.Equal(t, []byte("peer-a"), state.Parent())
	require.True(t, state.Ready())

	better := newSpanningTreeBeaconMsg([]byte("root"), 1, 2, 0, 1, []byte("peer-b"))
	ps = state.handleBeacon(better, []byte("peer-b"))
	require.True(t, ps.Changed)
	require.Equal(t, []byte("peer-a"), ps.OldParent)
	require.Equal(t, []byte("peer-b"), ps.NewParent)
}

func TestSpanningTreeExplicitChildrenAndDegreeBound(t *testing.T) {
	state := newSpanningTreeState([]byte("self"), true, 1)

	require.True(t, state.handleAttachRequest([]byte("child-a")))
	require.False(t, state.handleAttachRequest([]byte("child-b")), "degree bound should reject second child")
	require.Equal(t, 1, state.ChildCount())

	state.handleDetach([]byte("child-a"))
	require.Equal(t, 0, state.ChildCount())
	require.True(t, state.handleAttachRequest([]byte("child-b")))
}

func TestSpanningTreeSelectsOnlyExplicitChildren(t *testing.T) {
	state := newSpanningTreeState([]byte("self"), true, 8)
	require.True(t, state.handleAttachRequest([]byte("peer-a")))
	require.True(t, state.handleAttachRequest([]byte("peer-b")))

	// Beacons from other peers must NOT create children.
	state.handleBeacon(newSpanningTreeBeaconMsg([]byte("root"), 1, 1, 2, 50, []byte("peer-c")), []byte("peer-c"))

	peers := []*comm.RemotePeer{
		{PKIID: []byte("peer-a")},
		{PKIID: []byte("peer-c")},
		{PKIID: []byte("peer-b")},
	}
	selected := state.selectPeers(peers)
	require.Len(t, selected, 2)
	var selectedPKIIDs [][]byte
	for _, peer := range selected {
		selectedPKIIDs = append(selectedPKIIDs, peer.PKIID)
	}
	require.ElementsMatch(t, [][]byte{[]byte("peer-a"), []byte("peer-b")}, selectedPKIIDs)
}

func TestSpanningTreeReadyRequiresRootOrAcceptedParent(t *testing.T) {
	state := newSpanningTreeState([]byte("self"), true, 8)
	require.False(t, state.Ready())

	state.BecomeRoot(1)
	require.True(t, state.IsRoot())
	require.True(t, state.Ready())

	state.RelinquishRoot()
	require.False(t, state.Ready())

	state.handleBeacon(newSpanningTreeBeaconMsg([]byte("root"), 1, 1, 0, 0, []byte("parent")), []byte("parent"))
	require.False(t, state.Ready(), "pending attach is not ready")
	state.handleAttachAccept([]byte("parent"))
	require.True(t, state.Ready())
}

func TestSpanningTreeOnPeerDeadClearsParent(t *testing.T) {
	state := newSpanningTreeState([]byte("self"), true, 8)
	state.handleBeacon(newSpanningTreeBeaconMsg([]byte("root"), 1, 1, 0, 0, []byte("parent")), []byte("parent"))
	state.handleAttachAccept([]byte("parent"))
	require.True(t, state.OnPeerDead([]byte("parent")))
	require.False(t, state.Ready())
}

func TestPickRootPKIID(t *testing.T) {
	self := []byte("m")
	require.Equal(t, []byte("a"), pickRootPKIID(self, [][]byte{[]byte("z"), []byte("a")}))
	require.Equal(t, self, pickRootPKIID(self, nil))
}

func TestGossipInChanSendsMarkedBlockMessagesOnlyToSpanningTreeChildren(t *testing.T) {
	tree := newSpanningTreeState([]byte("self"), true, 8)
	tree.BecomeRoot(1)
	require.True(t, tree.handleAttachRequest([]byte("peer-a")))
	require.True(t, tree.handleAttachRequest([]byte("peer-b")))

	node := &Node{
		spanningTree: tree,
		conf:         &Config{PropagateIterations: 1, EnableSpanningTree: true},
		logger:       util.GetLogger(util.GossipLogger, "test"),
	}

	msg := &emittedGossipMessage{
		SignedGossipMessage: &protoext.SignedGossipMessage{
			GossipMessage: &pg.GossipMessage{
				Channel: []byte("A"),
				Content: &pg.GossipMessage_DataMsg{
					DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: 1}},
				},
			},
		},
		filter:               func(_ common.PKIidType) bool { return true },
		routeViaSpanningTree: true,
	}

	var sentTo []*comm.RemotePeer
	node.comm = &mockComm{sendFn: func(_ *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
		sentTo = append(sentTo, peers...)
	}}
	node.chanState = &channelState{channels: map[string]channel.GossipChannel{
		"A": &mockGossipChannel{},
	}}
	node.disc = &mockDiscovery{members: []discovery.NetworkMember{
		{PKIid: []byte("peer-a")},
		{PKIid: []byte("peer-b")},
		{PKIid: []byte("peer-c")},
	}}

	node.gossipInChan([]*emittedGossipMessage{msg}, func(gc channel.GossipChannel) filter.RoutingFilter {
		return func(member discovery.NetworkMember) bool { return true }
	})

	require.Len(t, sentTo, 2)
	var sentPKIIDs [][]byte
	for _, peer := range sentTo {
		sentPKIIDs = append(sentPKIIDs, peer.PKIID)
	}
	require.ElementsMatch(t, [][]byte{[]byte("peer-a"), []byte("peer-b")}, sentPKIIDs)
}

func TestShouldRouteViaSpanningTreeRequiresReadiness(t *testing.T) {
	tree := newSpanningTreeState([]byte("self"), true, 8)
	node := &Node{
		spanningTree: tree,
		conf:         &Config{EnableSpanningTree: true},
	}
	msg := &pg.GossipMessage{
		Channel: []byte("A"),
		Content: &pg.GossipMessage_DataMsg{
			DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: 1}},
		},
	}
	require.False(t, node.shouldRouteViaSpanningTree(msg))
	tree.BecomeRoot(1)
	require.True(t, node.shouldRouteViaSpanningTree(msg))
}

func TestRTTToLinkCost(t *testing.T) {
	require.Equal(t, uint32(1), rttToLinkCost(0))
	require.Equal(t, uint32(5), rttToLinkCost(5*time.Millisecond))
}

type mockComm struct {
	sendFn func(msg *protoext.SignedGossipMessage, peers ...*comm.RemotePeer)
}

func (m *mockComm) GetPKIid() common.PKIidType { return nil }
func (m *mockComm) Send(msg *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
	if m.sendFn != nil {
		m.sendFn(msg, peers...)
	}
}

func (m *mockComm) SendWithAck(msg *protoext.SignedGossipMessage, _ time.Duration, _ int, peers ...*comm.RemotePeer) comm.AggregatedSendResult {
	return nil
}
func (m *mockComm) Probe(peer *comm.RemotePeer) error                             { return nil }
func (m *mockComm) Handshake(peer *comm.RemotePeer) (api.PeerIdentityType, error) { return nil, nil }
func (m *mockComm) Accept(common.MessageAcceptor) <-chan protoext.ReceivedMessage { return nil }
func (m *mockComm) PresumedDead() <-chan common.PKIidType                         { return nil }
func (m *mockComm) IdentitySwitch() chan common.PKIidType                         { return nil }
func (m *mockComm) CloseConn(peer *comm.RemotePeer)                               {}
func (m *mockComm) Stop()                                                         {}

type mockDiscovery struct {
	discovery.Discovery
	members []discovery.NetworkMember
}

func (m *mockDiscovery) GetMembership() []discovery.NetworkMember { return m.members }

type mockGossipChannel struct{}

func (m *mockGossipChannel) Self() *protoext.SignedGossipMessage { return nil }
func (m *mockGossipChannel) GetPeers() []discovery.NetworkMember { return nil }
func (m *mockGossipChannel) PeerFilter(api.SubChannelSelectionCriteria) filter.RoutingFilter {
	return nil
}
func (m *mockGossipChannel) IsMemberInChan(discovery.NetworkMember) bool     { return true }
func (m *mockGossipChannel) UpdateLedgerHeight(uint64)                       {}
func (m *mockGossipChannel) UpdateChaincodes([]*pg.Chaincode)                {}
func (m *mockGossipChannel) IsOrgInChannel(api.OrgIdentityType) bool         { return true }
func (m *mockGossipChannel) EligibleForChannel(discovery.NetworkMember) bool { return true }
func (m *mockGossipChannel) HandleMessage(protoext.ReceivedMessage)          {}
func (m *mockGossipChannel) AddToMsgStore(*protoext.SignedGossipMessage)     {}
func (m *mockGossipChannel) ConfigureChannel(api.JoinChannelMessage)         {}
func (m *mockGossipChannel) LeaveChannel()                                   {}
func (m *mockGossipChannel) Stop()                                           {}
