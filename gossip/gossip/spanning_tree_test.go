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

func TestSpanningTreeStateAdoptsBestParent(t *testing.T) {
	state := newSpanningTreeState()

	first := &pg.GossipMessage{
		Content: &pg.GossipMessage_SpanningTree{
			SpanningTree: &pg.SpanningTreeMsg{
				RootPkiId:   []byte("root"),
				RootIncNum:  1,
				RootSeqNum:  1,
				Distance:    1,
				SenderPkiId: []byte("peer-a"),
				PathCost:    10,
			},
		},
	}

	require.True(t, state.handle(first, []byte("peer-a")))
	require.Equal(t, []byte("peer-a"), state.parent)
	require.Equal(t, uint32(10), state.bestPathCost)

	better := &pg.GossipMessage{
		Content: &pg.GossipMessage_SpanningTree{
			SpanningTree: &pg.SpanningTreeMsg{
				RootPkiId:   []byte("root"),
				RootIncNum:  1,
				RootSeqNum:  1,
				Distance:    0,
				SenderPkiId: []byte("peer-b"),
				PathCost:    1,
			},
		},
	}

	require.True(t, state.handle(better, []byte("peer-b")))
	require.Equal(t, []byte("peer-b"), state.parent)
	require.Equal(t, uint32(1), state.bestPathCost)
}

func TestSpanningTreeStateSelectsChildPeersForData(t *testing.T) {
	state := newSpanningTreeState()
	state.children["peer-a"] = struct{}{}
	state.children["peer-b"] = struct{}{}

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

func TestGossipInChanSendsMarkedBlockMessagesOnlyToSpanningTreeChildren(t *testing.T) {
	node := &Node{
		spanningTree: newSpanningTreeState(),
		conf:         &Config{PropagateIterations: 1},
		logger:       util.GetLogger(util.GossipLogger, "test"),
	}
	node.spanningTree.children[string([]byte("peer-a"))] = struct{}{}
	node.spanningTree.children[string([]byte("peer-b"))] = struct{}{}

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
