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

func TestGossipInChanSendsBlockToAllSameOrgChannelPeers(t *testing.T) {
	node := newBlockDisseminationTestNode(true)
	msg := newEmittedBlockMsg("A", 1)

	var sentTo []*comm.RemotePeer
	node.comm = &blockDisseminationMockComm{sendFn: func(_ *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
		sentTo = append(sentTo, peers...)
	}}

	node.gossipInChan([]*emittedGossipMessage{msg}, func(gc channel.GossipChannel) filter.RoutingFilter {
		return func(member discovery.NetworkMember) bool { return true }
	})

	require.Len(t, sentTo, 3)
	require.ElementsMatch(t, [][]byte{[]byte("peer-a"), []byte("peer-b"), []byte("peer-c")}, pkiIDs(sentTo))
}

func TestGossipInChanUsesClassicFanoutWhenOrgBlockDisseminationDisabled(t *testing.T) {
	node := newBlockDisseminationTestNode(false)
	node.conf.PropagatePeerNum = 1
	msg := newEmittedBlockMsg("A", 1)

	var sentTo []*comm.RemotePeer
	node.comm = &blockDisseminationMockComm{sendFn: func(_ *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
		sentTo = append(sentTo, peers...)
	}}

	node.gossipInChan([]*emittedGossipMessage{msg}, func(gc channel.GossipChannel) filter.RoutingFilter {
		return func(member discovery.NetworkMember) bool { return true }
	})

	require.Len(t, sentTo, 1)
}

func TestForwardDoesNotRegossipOrgDisseminatedBlocks(t *testing.T) {
	node := newBlockDisseminationTestNode(true)
	adapter := &gossipAdapterImpl{Node: node}
	var emitted bool
	node.emitter = newBatchingEmitter(1, 10, time.Hour, func(a []interface{}) {
		emitted = true
	})
	t.Cleanup(node.emitter.Stop)

	adapter.Forward(&blockDisseminationReceivedMsg{
		msg: &protoext.SignedGossipMessage{
			GossipMessage: &pg.GossipMessage{
				Channel: []byte("A"),
				Tag:     pg.GossipMessage_CHAN_AND_ORG,
				Content: &pg.GossipMessage_DataMsg{
					DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: 7}},
				},
			},
		},
		sender: []byte("peer-a"),
	})

	require.False(t, emitted)
	require.Zero(t, node.emitter.Size())
}

func TestForwardStillGossipsNonBlockMessages(t *testing.T) {
	node := newBlockDisseminationTestNode(true)
	adapter := &gossipAdapterImpl{Node: node}
	node.emitter = newBatchingEmitter(1, 10, time.Hour, func(a []interface{}) {})
	t.Cleanup(node.emitter.Stop)

	adapter.Forward(&blockDisseminationReceivedMsg{
		msg: &protoext.SignedGossipMessage{
			GossipMessage: &pg.GossipMessage{
				Channel: []byte("A"),
				Tag:     pg.GossipMessage_CHAN_AND_ORG,
				Content: &pg.GossipMessage_LeadershipMsg{
					LeadershipMsg: &pg.LeadershipMessage{},
				},
			},
		},
		sender: []byte("peer-a"),
	})

	require.Equal(t, 1, node.emitter.Size())
}

func newBlockDisseminationTestNode(enabled bool) *Node {
	return &Node{
		conf:   &Config{PropagateIterations: 1, PropagatePeerNum: 3, OrgBlockDissemination: enabled},
		logger: util.GetLogger(util.GossipLogger, "test"),
		chanState: &channelState{channels: map[string]channel.GossipChannel{
			"A": &blockDisseminationMockChannel{},
		}},
		disc: &blockDisseminationMockDiscovery{members: []discovery.NetworkMember{
			{PKIid: []byte("peer-a")},
			{PKIid: []byte("peer-b")},
			{PKIid: []byte("peer-c")},
		}},
	}
}

func newEmittedBlockMsg(channel string, seq uint64) *emittedGossipMessage {
	return &emittedGossipMessage{
		SignedGossipMessage: &protoext.SignedGossipMessage{
			GossipMessage: &pg.GossipMessage{
				Channel: []byte(channel),
				Tag:     pg.GossipMessage_CHAN_AND_ORG,
				Content: &pg.GossipMessage_DataMsg{
					DataMsg: &pg.DataMessage{Payload: &pg.Payload{SeqNum: seq}},
				},
			},
		},
		filter: func(_ common.PKIidType) bool { return true },
	}
}

func pkiIDs(peers []*comm.RemotePeer) [][]byte {
	ids := make([][]byte, 0, len(peers))
	for _, peer := range peers {
		ids = append(ids, peer.PKIID)
	}
	return ids
}

type blockDisseminationMockComm struct {
	sendFn func(msg *protoext.SignedGossipMessage, peers ...*comm.RemotePeer)
}

func (m *blockDisseminationMockComm) GetPKIid() common.PKIidType { return nil }
func (m *blockDisseminationMockComm) Send(msg *protoext.SignedGossipMessage, peers ...*comm.RemotePeer) {
	if m.sendFn != nil {
		m.sendFn(msg, peers...)
	}
}
func (m *blockDisseminationMockComm) SendWithAck(msg *protoext.SignedGossipMessage, _ time.Duration, _ int, peers ...*comm.RemotePeer) comm.AggregatedSendResult {
	return nil
}
func (m *blockDisseminationMockComm) Probe(peer *comm.RemotePeer) error {
	return nil
}
func (m *blockDisseminationMockComm) Handshake(peer *comm.RemotePeer) (api.PeerIdentityType, error) {
	return nil, nil
}
func (m *blockDisseminationMockComm) Accept(common.MessageAcceptor) <-chan protoext.ReceivedMessage {
	return nil
}
func (m *blockDisseminationMockComm) PresumedDead() <-chan common.PKIidType { return nil }
func (m *blockDisseminationMockComm) IdentitySwitch() chan common.PKIidType { return nil }
func (m *blockDisseminationMockComm) CloseConn(peer *comm.RemotePeer)       {}
func (m *blockDisseminationMockComm) Stop()                                 {}

type blockDisseminationMockDiscovery struct {
	discovery.Discovery
	members []discovery.NetworkMember
}

func (m *blockDisseminationMockDiscovery) GetMembership() []discovery.NetworkMember {
	return m.members
}

type blockDisseminationMockChannel struct{}

func (m *blockDisseminationMockChannel) Self() *protoext.SignedGossipMessage { return nil }
func (m *blockDisseminationMockChannel) GetPeers() []discovery.NetworkMember { return nil }
func (m *blockDisseminationMockChannel) PeerFilter(api.SubChannelSelectionCriteria) filter.RoutingFilter {
	return nil
}
func (m *blockDisseminationMockChannel) IsMemberInChan(discovery.NetworkMember) bool     { return true }
func (m *blockDisseminationMockChannel) UpdateLedgerHeight(uint64)                       {}
func (m *blockDisseminationMockChannel) UpdateChaincodes([]*pg.Chaincode)                {}
func (m *blockDisseminationMockChannel) IsOrgInChannel(api.OrgIdentityType) bool         { return true }
func (m *blockDisseminationMockChannel) EligibleForChannel(discovery.NetworkMember) bool { return true }
func (m *blockDisseminationMockChannel) HandleMessage(protoext.ReceivedMessage)          {}
func (m *blockDisseminationMockChannel) AddToMsgStore(*protoext.SignedGossipMessage)     {}
func (m *blockDisseminationMockChannel) ConfigureChannel(api.JoinChannelMessage)         {}
func (m *blockDisseminationMockChannel) LeaveChannel()                                   {}
func (m *blockDisseminationMockChannel) Stop()                                           {}

type blockDisseminationReceivedMsg struct {
	msg    *protoext.SignedGossipMessage
	sender common.PKIidType
}

func (m *blockDisseminationReceivedMsg) GetSourceEnvelope() *pg.Envelope {
	return nil
}
func (m *blockDisseminationReceivedMsg) GetGossipMessage() *protoext.SignedGossipMessage {
	return m.msg
}
func (m *blockDisseminationReceivedMsg) GetConnectionInfo() *protoext.ConnectionInfo {
	return &protoext.ConnectionInfo{ID: m.sender}
}
func (m *blockDisseminationReceivedMsg) Respond(msg *pg.GossipMessage) {}
func (m *blockDisseminationReceivedMsg) Ack(error)                     {}
